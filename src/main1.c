#include <stdio.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/usb_serial_jtag.h"
#include "nvs_flash.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_now.h"
#include "esp_mac.h"
#include "esp_timer.h"
#include "freertos/semphr.h"

/* ── compile-time constants ─────────────────────────────────────────────── */
#define BUF_SIZE             1024
#define WIFI_CHANNEL         1
#define MSG_TTL              5
#define INITIAL_TTL          MSG_TTL
#define DEDUP_SIZE           32      // увеличен: unicast дублей меньше, но
                                     // ANNOUNCE по-прежнему flood

#define MAX_NODES            16
#define ANNOUNCE_INTERVAL_MS 3000
#define NODE_TIMEOUT_MS      10000
#define REAPER_INTERVAL_MS   1000
#define MAX_NAME_LEN         16
#define MAX_PATH_HOPS        6

#define USER_NAME            "Adilet"

#define MSG_TYPE_DATA        0
#define MSG_TYPE_ANNOUNCE    1

/* ── wire format (unchanged, binary-compatible со старой версией) ────────── */
typedef struct __attribute__((packed)) {
    uint16_t magic;
    uint8_t  origin[6];
    uint16_t seq;
    uint8_t  ttl;
    uint8_t  msg_type;
    uint8_t  dest[6];
    uint8_t  next_hop[6];
    uint8_t  path_len;
    uint8_t  path[MAX_PATH_HOPS][6];
    char     text[180];
} chat_msg_t;

#define CHAT_MAGIC 0xC4A7

static const uint8_t BROADCAST_MAC[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
static uint8_t s_my_mac[6];

/* ── sequence counter ────────────────────────────────────────────────────── */
static uint16_t        s_my_seq  = 0;
static portMUX_TYPE    s_seq_mux = portMUX_INITIALIZER_UNLOCKED;

static uint16_t next_seq(void)
{
    taskENTER_CRITICAL(&s_seq_mux);
    uint16_t s = ++s_my_seq;
    taskEXIT_CRITICAL(&s_seq_mux);
    return s;
}

/* ── dedup ring ──────────────────────────────────────────────────────────── */
static struct { uint8_t mac[6]; uint16_t seq; } s_seen[DEDUP_SIZE];
static int s_seen_pos = 0;

static bool already_seen(const uint8_t *mac, uint16_t seq)
{
    for (int i = 0; i < DEDUP_SIZE; i++) {
        if (s_seen[i].seq == seq && memcmp(s_seen[i].mac, mac, 6) == 0)
            return true;
    }
    memcpy(s_seen[s_seen_pos].mac, mac, 6);
    s_seen[s_seen_pos].seq = seq;
    s_seen_pos = (s_seen_pos + 1) % DEDUP_SIZE;
    return false;
}

/* ── node table ──────────────────────────────────────────────────────────── */
typedef struct {
    bool     in_use;
    uint8_t  mac[6];
    uint8_t  next_hop[6];
    char     name[MAX_NAME_LEN];
    int8_t   rssi;
    bool     rssi_valid;
    uint8_t  hops;
    uint32_t last_seen_ms;
    uint16_t last_seq;
    bool     peer_added;    // ← NEW: флаг «peer уже зарегистрирован в ESP-NOW»
} node_entry_t;

static node_entry_t    s_nodes[MAX_NODES];
static SemaphoreHandle_t s_nodes_mtx;

static inline uint32_t now_ms(void)
{
    return (uint32_t)(esp_timer_get_time() / 1000);
}

/* ── peer manager ────────────────────────────────────────────────────────── */
/*
 * ESP-NOW требует, чтобы адрес назначения был заранее добавлен через
 * esp_now_add_peer(). Мы управляем этим динамически:
 *   - При появлении нового next_hop-соседа добавляем его как peer.
 *   - При истечении записи удаляем его.
 *
 * ВАЖНО: peer — это непосредственный сосед (next_hop), а не конечный
 * получатель. Для multi-hop достаточно знать только следующий прыжок.
 */
static void peer_ensure_added(const uint8_t *mac)
{
    if (esp_now_is_peer_exist(mac)) return;

    esp_now_peer_info_t p = {
        .channel = WIFI_CHANNEL,
        .ifidx   = WIFI_IF_STA,
        .encrypt = false,
    };
    memcpy(p.peer_addr, mac, 6);
    esp_err_t err = esp_now_add_peer(&p);
    if (err != ESP_OK && err != ESP_ERR_ESPNOW_EXIST) {
        printf("[peer] add %02X:%02X:%02X:%02X:%02X:%02X failed: %s\n",
               mac[0], mac[1], mac[2], mac[3], mac[4], mac[5],
               esp_err_to_name(err));
    }
}

static void peer_remove(const uint8_t *mac)
{
    if (!esp_now_is_peer_exist(mac)) return;
    esp_now_del_peer(mac);
}

/* ── send helpers ────────────────────────────────────────────────────────── */

/* Broadcast используется только для ANNOUNCE */
static void broadcast(const chat_msg_t *msg)
{
    size_t len = sizeof(*msg) - sizeof(msg->text) + strlen(msg->text) + 1;
    esp_err_t err = esp_now_send(BROADCAST_MAC, (const uint8_t *)msg, len);
    if (err != ESP_OK)
        printf("broadcast failed: %s\n", esp_err_to_name(err));
}

/*
 * Unicast: отправить фрейм конкретному соседу.
 * Перед отправкой убеждаемся, что peer добавлен.
 * Возвращает true при успехе.
 */
static bool send_unicast(const uint8_t *neighbor_mac, const chat_msg_t *msg)
{
    peer_ensure_added(neighbor_mac);
    size_t len = sizeof(*msg) - sizeof(msg->text) + strlen(msg->text) + 1;
    esp_err_t err = esp_now_send(neighbor_mac, (const uint8_t *)msg, len);
    if (err != ESP_OK) {
        printf("unicast to %02X:%02X:%02X:%02X:%02X:%02X failed: %s\n",
               neighbor_mac[0], neighbor_mac[1], neighbor_mac[2],
               neighbor_mac[3], neighbor_mac[4], neighbor_mac[5],
               esp_err_to_name(err));
        return false;
    }
    return true;
}

/* ── table_update ────────────────────────────────────────────────────────── */
/*
 * Обновляем таблицу узлов при получении ANNOUNCE.
 * Логика многопутевого обнаружения — без изменений.
 * Новое: при появлении нового прямого соседа (hops==1) сразу
 * добавляем его MAC как ESP-NOW peer.
 */
static void table_update(const uint8_t *origin, const char *name,
                         const uint8_t *via,    uint16_t seq,
                         uint8_t hops,          int8_t rssi)
{
    if (!s_nodes_mtx) return;
    xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);

    uint32_t now = now_ms();
    int idx = -1, free_idx = -1;
    for (int i = 0; i < MAX_NODES; i++) {
        if (s_nodes[i].in_use) {
            if (memcmp(s_nodes[i].mac, origin, 6) == 0) { idx = i; break; }
        } else if (free_idx < 0) {
            free_idx = i;
        }
    }

    if (idx < 0) {
        if (free_idx < 0) { xSemaphoreGive(s_nodes_mtx); return; }
        node_entry_t *n = &s_nodes[free_idx];
        memset(n, 0, sizeof(*n));
        n->in_use       = true;
        memcpy(n->mac,      origin, 6);
        memcpy(n->next_hop, via,    6);
        snprintf(n->name, sizeof(n->name), "%s", name);
        n->last_seq     = seq;
        n->hops         = hops;
        n->rssi_valid   = (hops == 1);
        if (hops == 1) n->rssi = rssi;
        n->last_seen_ms = now;
        n->peer_added   = false;
        xSemaphoreGive(s_nodes_mtx);

        /* Прямой сосед — добавляем как peer вне lock'а */
        if (hops == 1) peer_ensure_added(origin);
        return;
    }

    node_entry_t *n = &s_nodes[idx];
    n->last_seen_ms = now;
    snprintf(n->name, sizeof(n->name), "%s", name);

    int16_t d = (int16_t)(seq - n->last_seq);
    if (d > 0) {
        n->last_seq   = seq;
        n->hops       = hops;
        memcpy(n->next_hop, via, 6);
        n->rssi_valid = (hops == 1);
        if (hops == 1) n->rssi = rssi;
    } else if (d == 0) {
        if (hops < n->hops) { n->hops = hops; memcpy(n->next_hop, via, 6); }
        if (hops == 1)      { n->rssi = rssi; n->rssi_valid = true; }
    }

    bool need_peer = (hops == 1);
    xSemaphoreGive(s_nodes_mtx);

    if (need_peer) peer_ensure_added(origin);
}

/* ── routing lookups ─────────────────────────────────────────────────────── */
static bool route_by_name(const char *name, uint8_t *dest, uint8_t *next_hop)
{
    bool found = false;
    if (!s_nodes_mtx) return false;
    xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);
    for (int i = 0; i < MAX_NODES; i++) {
        if (s_nodes[i].in_use && strcmp(s_nodes[i].name, name) == 0) {
            memcpy(dest,     s_nodes[i].mac,      6);
            memcpy(next_hop, s_nodes[i].next_hop, 6);
            found = true;
            break;
        }
    }
    xSemaphoreGive(s_nodes_mtx);
    return found;
}

static bool route_by_mac(const uint8_t *dest, uint8_t *next_hop)
{
    bool found = false;
    if (!s_nodes_mtx) return false;
    xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);
    for (int i = 0; i < MAX_NODES; i++) {
        if (s_nodes[i].in_use && memcmp(s_nodes[i].mac, dest, 6) == 0) {
            memcpy(next_hop, s_nodes[i].next_hop, 6);
            found = true;
            break;
        }
    }
    xSemaphoreGive(s_nodes_mtx);
    return found;
}

static void path_append(chat_msg_t *m, const uint8_t *mac)
{
    if (m->path_len < MAX_PATH_HOPS) {
        memcpy(m->path[m->path_len], mac, 6);
        m->path_len++;
    }
}

static void name_by_mac(const uint8_t *mac, char *out, size_t out_sz)
{
    if (memcmp(mac, s_my_mac, 6) == 0) {
        snprintf(out, out_sz, "%s", USER_NAME);
        return;
    }
    if (s_nodes_mtx) {
        xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);
        for (int i = 0; i < MAX_NODES; i++) {
            if (s_nodes[i].in_use && memcmp(s_nodes[i].mac, mac, 6) == 0) {
                snprintf(out, out_sz, "%s", s_nodes[i].name);
                xSemaphoreGive(s_nodes_mtx);
                return;
            }
        }
        xSemaphoreGive(s_nodes_mtx);
    }
    snprintf(out, out_sz, "%02X:%02X:%02X:%02X:%02X:%02X",
             mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

/* ── receive callback ────────────────────────────────────────────────────── */
static void on_recv(const esp_now_recv_info_t *info,
                    const uint8_t *data, int len)
{
    if (len < (int)(sizeof(chat_msg_t) - sizeof(((chat_msg_t *)0)->text) + 1))
        return;

    chat_msg_t msg;
    memcpy(&msg, data, len > (int)sizeof(msg) ? sizeof(msg) : len);
    msg.text[sizeof(msg.text) - 1] = '\0';

    if (msg.magic != CHAT_MAGIC) return;
    if (memcmp(msg.origin, s_my_mac, 6) == 0) return;  // own echo

    /* ── ANNOUNCE: flood + discovery ──────────────────────────────────── */
    if (msg.msg_type == MSG_TYPE_ANNOUNCE) {
        uint8_t hops = (uint8_t)(INITIAL_TTL - msg.ttl + 1);
        int8_t  rssi = info->rx_ctrl->rssi;

        /* Обновляем таблицу ДО проверки already_seen,
           чтобы каждый путь мог улучшить метрики. */
        table_update(msg.origin, msg.text, info->src_addr,
                     msg.seq, hops, rssi);

        if (already_seen(msg.origin, msg.seq)) return;
        if (msg.ttl > 0) {
            msg.ttl--;
            broadcast(&msg);   // ANNOUNCE остаётся flood-broadcast
        }
        return;
    }

    /* ── DATA: hop-by-hop unicast ─────────────────────────────────────── */
    /*
     * Проверяем, что фрейм адресован именно нам как next_hop.
     * В отличие от broadcast-версии здесь это избыточно (ESP-NOW unicast
     * физически доставит фрейм только нам), но оставляем как защиту
     * от ошибок маршрутизации.
     */
    if (memcmp(msg.next_hop, s_my_mac, 6) != 0) return;
    if (already_seen(msg.origin, msg.seq)) return;

    /* Мы — конечный получатель */
    if (memcmp(msg.dest, s_my_mac, 6) == 0) {
        path_append(&msg, s_my_mac);

        char path_str[160];
        size_t pos = 0;
        path_str[0] = '\0';
        for (int k = 0; k < msg.path_len; k++) {
            char nm[18];
            name_by_mac(msg.path[k], nm, sizeof(nm));
            pos += snprintf(path_str + pos, sizeof(path_str) - pos,
                            "%s%s", (k == 0) ? "" : " -> ", nm);
            if (pos >= sizeof(path_str)) break;
        }
        printf("[%s] %s\n", path_str, msg.text);
        return;
    }

    /* Промежуточный hop: пересылаем unicast'ом следующему соседу */
    if (msg.ttl == 0) return;
    uint8_t nh[6];
    if (!route_by_mac(msg.dest, nh)) {
        printf("[relay] route to dest lost, drop\n");
        return;
    }
    path_append(&msg, s_my_mac);
    msg.ttl--;
    memcpy(msg.next_hop, nh, 6);

    /* ← Главное отличие: unicast вместо broadcast */
    send_unicast(nh, &msg);
}

/* ── announce task ───────────────────────────────────────────────────────── */
static void announce_task(void *arg)
{
    chat_msg_t msg = { .magic = CHAT_MAGIC, .msg_type = MSG_TYPE_ANNOUNCE };
    memcpy(msg.origin, s_my_mac, 6);
    snprintf(msg.text, sizeof(msg.text), "%s", USER_NAME);

    while (1) {
        msg.seq = next_seq();
        msg.ttl = INITIAL_TTL;
        broadcast(&msg);    // discovery всегда broadcast
        vTaskDelay(pdMS_TO_TICKS(ANNOUNCE_INTERVAL_MS));
    }
}

/* ── reaper task ─────────────────────────────────────────────────────────── */
/*
 * Удаляем просроченные записи из таблицы И из списка ESP-NOW peers.
 * Прямые соседи (hops==1) были добавлены как peers — их нужно убрать.
 */
static void reaper_task(void *arg)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(REAPER_INTERVAL_MS));
        uint32_t now = now_ms();

        xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);
        for (int i = 0; i < MAX_NODES; i++) {
            if (!s_nodes[i].in_use) continue;
            if ((now - s_nodes[i].last_seen_ms) > NODE_TIMEOUT_MS) {
                uint8_t mac[6];
                memcpy(mac, s_nodes[i].mac, 6);
                bool was_direct = (s_nodes[i].hops == 1);
                s_nodes[i].in_use = false;
                xSemaphoreGive(s_nodes_mtx);  // освобождаем до esp_now_del_peer

                if (was_direct) peer_remove(mac);

                xSemaphoreTake(s_nodes_mtx, portMAX_DELAY); // берём снова
            }
        }
        xSemaphoreGive(s_nodes_mtx);
    }
}

/* ── print_table ─────────────────────────────────────────────────────────── */
static void print_table(void)
{
    node_entry_t snap[MAX_NODES];
    uint32_t now;

    xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);
    now = now_ms();
    memcpy(snap, s_nodes, sizeof(snap));
    xSemaphoreGive(s_nodes_mtx);

    printf("%-3s%-20s%-12s%-6s%-6s%s\n",
           "#", "MAC", "NAME", "RSSI", "HOPS", "AGE");
    int row = 0;
    for (int i = 0; i < MAX_NODES; i++) {
        node_entry_t *n = &snap[i];
        if (!n->in_use) continue;

        uint32_t age_s = (now - n->last_seen_ms) / 1000;
        char mac_str[18];
        snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X",
                 n->mac[0], n->mac[1], n->mac[2],
                 n->mac[3], n->mac[4], n->mac[5]);
        char rssi_str[8];
        if (n->rssi_valid) snprintf(rssi_str, sizeof(rssi_str), "%d", n->rssi);
        else               snprintf(rssi_str, sizeof(rssi_str), "N/A");

        printf("%-3d%-20s%-12s%-6s%-6u%lus\n",
               row, mac_str, n->name, rssi_str, n->hops, (unsigned long)age_s);
        row++;
    }
    if (row == 0) printf("(no nodes seen yet)\n");
}

/* ── wifi / esp-now init ─────────────────────────────────────────────────── */
static void wifi_espnow_init(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_ERROR_CHECK(esp_wifi_set_channel(WIFI_CHANNEL, WIFI_SECOND_CHAN_NONE));
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));

    ESP_ERROR_CHECK(esp_now_init());
    ESP_ERROR_CHECK(esp_now_register_recv_cb(on_recv));

    /* Broadcast peer — только для ANNOUNCE */
    esp_now_peer_info_t peer = {
        .channel = WIFI_CHANNEL,
        .ifidx   = WIFI_IF_STA,
        .encrypt = false,
    };
    memcpy(peer.peer_addr, BROADCAST_MAC, 6);
    ESP_ERROR_CHECK(esp_now_add_peer(&peer));
}

/* ── app_main ────────────────────────────────────────────────────────────── */
void app_main(void)
{
    vTaskDelay(1000 / portTICK_PERIOD_MS);

    usb_serial_jtag_driver_config_t usj_config = {
        .tx_buffer_size = BUF_SIZE,
        .rx_buffer_size = BUF_SIZE,
    };
    ESP_ERROR_CHECK(usb_serial_jtag_driver_install(&usj_config));

    s_nodes_mtx = xSemaphoreCreateMutex();
    ESP_ERROR_CHECK(s_nodes_mtx ? ESP_OK : ESP_ERR_NO_MEM);

    wifi_espnow_init();

    esp_wifi_get_mac(WIFI_IF_STA, s_my_mac);
    printf("Mesh chat ready. MAC: %02X:%02X:%02X:%02X:%02X:%02X  ch%d\n",
           s_my_mac[0], s_my_mac[1], s_my_mac[2],
           s_my_mac[3], s_my_mac[4], s_my_mac[5], WIFI_CHANNEL);

    uint32_t version = 0;
    ESP_ERROR_CHECK(esp_now_get_version(&version));
    printf("ESP-NOW version: %ld\n", version);

    xTaskCreate(announce_task, "announce", 3072, NULL, 4, NULL);
    xTaskCreate(reaper_task,   "reaper",   2560, NULL, 3, NULL);

    printf("Commands:\n"
           "  list                 - show the node table\n"
           "  <name> <message>     - send <message> to <name>\n");

    char    line[256];
    size_t  line_len = 0;
    uint8_t chunk[64];

    while (1) {
        int n = usb_serial_jtag_read_bytes(chunk, sizeof(chunk),
                                           20 / portTICK_PERIOD_MS);
        for (int i = 0; i < n; i++) {
            char c = (char)chunk[i];
            if (c == '\n' || c == '\r') {
                if (line_len == 0) continue;
                line[line_len] = '\0';
                line_len = 0;

                if (strcmp(line, "list") == 0) {
                    print_table();
                    continue;
                }

                char *sp = strchr(line, ' ');
                if (!sp) { printf("usage: <name> <message>\n"); continue; }
                *sp = '\0';
                const char *target = line;
                const char *body   = sp + 1;

                uint8_t dest[6], nh[6];
                if (!route_by_name(target, dest, nh)) {
                    printf("user \"%s\" not found (try \"list\")\n", target);
                    continue;
                }

                chat_msg_t out = {
                    .magic    = CHAT_MAGIC,
                    .msg_type = MSG_TYPE_DATA,
                };
                memcpy(out.origin,   s_my_mac, 6);
                memcpy(out.dest,     dest,     6);
                memcpy(out.next_hop, nh,       6);
                out.seq = next_seq();
                out.ttl = MSG_TTL;
                path_append(&out, s_my_mac);
                snprintf(out.text, sizeof(out.text), "%s", body);

                /* Первый hop — unicast к непосредственному соседу */
                send_unicast(nh, &out);
                printf("[me -> %s] %s\n", target, body);

            } else if (line_len < sizeof(line) - 1) {
                line[line_len++] = c;
            }
        }
    }
}