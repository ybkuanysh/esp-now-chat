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

#define BUF_SIZE        1024
#define WIFI_CHANNEL    1   // must be the same on all boards
#define MSG_TTL         5   // max hops through the mesh
#define INITIAL_TTL     MSG_TTL  // ttl an ANNOUNCE starts with (used by hops formula)
#define DEDUP_SIZE      16  // remembered messages for flood suppression

// ---- node discovery configuration ----
#define MAX_NODES            16      // fixed-size visible-node table, no malloc
#define ANNOUNCE_INTERVAL_MS 3000    // how often we shout our presence
#define NODE_TIMEOUT_MS      10000   // drop a node after ~3 missed announces
#define REAPER_INTERVAL_MS   1000    // how often we scan for stale nodes
#define MAX_NAME_LEN         16      // stored/advertised user name length incl. NUL

// This node's human-readable name. Change it per board; it is broadcast inside
// every ANNOUNCE so neighbours can label us in their node tables.
#define USER_NAME            "kuanysh"

// message kinds carried in chat_msg_t.msg_type
#define MSG_TYPE_DATA     0   // user chat text (default => existing behaviour)
#define MSG_TYPE_ANNOUNCE 1   // presence beacon for node discovery

// ESP-NOW payload is limited to 250 bytes total
typedef struct __attribute__((packed)) {
    uint16_t magic;     // filter out foreign ESP-NOW traffic
    uint8_t origin[6];  // MAC of the board that wrote the message
    uint16_t seq;       // per-origin message counter
    uint8_t ttl;
    uint8_t msg_type;   // MSG_TYPE_DATA / MSG_TYPE_ANNOUNCE
    char text[200];     // must stay last: broadcast() trims trailing unused bytes
} chat_msg_t;

#define CHAT_MAGIC 0xC4A7

static const uint8_t BROADCAST_MAC[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
static uint8_t s_my_mac[6];
static uint16_t s_my_seq = 0;

// ring buffer of already seen (origin, seq) pairs
static struct { uint8_t mac[6]; uint16_t seq; } s_seen[DEDUP_SIZE];
static int s_seen_pos = 0;

static bool already_seen(const uint8_t *mac, uint16_t seq)
{
    for (int i = 0; i < DEDUP_SIZE; i++) {
        if (s_seen[i].seq == seq && memcmp(s_seen[i].mac, mac, 6) == 0) {
            return true;
        }
    }
    memcpy(s_seen[s_seen_pos].mac, mac, 6);
    s_seen[s_seen_pos].seq = seq;
    s_seen_pos = (s_seen_pos + 1) % DEDUP_SIZE;
    return false;
}

static void broadcast(const chat_msg_t *msg)
{
    size_t len = sizeof(*msg) - sizeof(msg->text) + strlen(msg->text) + 1;
    esp_err_t err = esp_now_send(BROADCAST_MAC, (const uint8_t *)msg, len);
    if (err != ESP_OK) {
        printf("esp_now_send failed: %s\n", esp_err_to_name(err));
    }
}

// ---------------------------------------------------------------------------
// Node discovery table
// ---------------------------------------------------------------------------
typedef struct {
    bool     in_use;        // slot occupied?
    uint8_t  mac[6];        // origin MAC of the node
    char     name[MAX_NAME_LEN]; // user name advertised by the node
    int8_t   rssi;          // last RSSI, only meaningful when rssi_valid
    bool     rssi_valid;    // true only while we hear the node directly (hops == 1)
    uint8_t  hops;          // minimum observed hop distance to the node
    uint32_t last_seen_ms;  // timestamp of the last copy we received
    uint16_t last_seq;      // newest announce round seen from this node
} node_entry_t;

static node_entry_t s_nodes[MAX_NODES];   // fixed table, no dynamic memory
static SemaphoreHandle_t s_nodes_mtx;     // guards s_nodes (taken in Wi-Fi task!)

// seq is bumped from both the console task and the announce task -> protect it.
static portMUX_TYPE s_seq_mux = portMUX_INITIALIZER_UNLOCKED;

static inline uint32_t now_ms(void)
{
    return (uint32_t)(esp_timer_get_time() / 1000);
}

static uint16_t next_seq(void)
{
    taskENTER_CRITICAL(&s_seq_mux);
    uint16_t s = ++s_my_seq;
    taskEXIT_CRITICAL(&s_seq_mux);
    return s;
}

// Update the visible-node table from one received ANNOUNCE copy.
// Called from on_recv (Wi-Fi task) for EVERY copy, before the relay decision.
//
// Multi-path handling:
//  - last_seen_ms is refreshed on every copy (keeps AGE accurate).
//  - A strictly newer seq starts a fresh announce round: adopt its hops/rssi.
//  - The same seq arriving via another path lets us lower hops to the minimum
//    and capture a direct-neighbour RSSI if this copy happens to be 1 hop.
//  - RSSI is only kept while hops == 1; for far nodes it stays invalid (N/A).
static void table_update(const uint8_t *origin, const char *name,
                         uint16_t seq, uint8_t hops, int8_t rssi)
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
        // First time we see this node; create an entry if there's room.
        if (free_idx < 0) { xSemaphoreGive(s_nodes_mtx); return; } // table full
        node_entry_t *n = &s_nodes[free_idx];
        memset(n, 0, sizeof(*n));
        n->in_use       = true;
        memcpy(n->mac, origin, 6);
        snprintf(n->name, sizeof(n->name), "%s", name);
        n->last_seq     = seq;
        n->hops         = hops;
        n->rssi_valid   = (hops == 1);
        if (hops == 1) n->rssi = rssi;
        n->last_seen_ms = now;
        xSemaphoreGive(s_nodes_mtx);
        return;
    }

    node_entry_t *n = &s_nodes[idx];
    n->last_seen_ms = now;                       // refresh AGE on ANY copy
    snprintf(n->name, sizeof(n->name), "%s", name); // name may change at runtime

    int16_t d = (int16_t)(seq - n->last_seq);    // wrap-safe seq comparison
    if (d > 0) {
        // Newer announce round: take its values as the new baseline.
        n->last_seq     = seq;
        n->hops         = hops;
        n->rssi_valid   = (hops == 1);
        if (hops == 1) n->rssi = rssi;
    } else if (d == 0) {
        // Same round via a different path: prefer the shortest route...
        if (hops < n->hops) n->hops = hops;
        // ...and grab RSSI if this particular copy reached us directly.
        if (hops == 1) { n->rssi = rssi; n->rssi_valid = true; }
    }
    // d < 0 (stale copy): nothing but the age refresh above.

    xSemaphoreGive(s_nodes_mtx);
}

static void on_recv(const esp_now_recv_info_t *info, const uint8_t *data, int len)
{
    if (len < (int)(sizeof(chat_msg_t) - sizeof(((chat_msg_t *)0)->text) + 1)) {
        return;
    }
    chat_msg_t msg;
    memcpy(&msg, data, len > (int)sizeof(msg) ? sizeof(msg) : len);
    msg.text[sizeof(msg.text) - 1] = '\0';

    if (msg.magic != CHAT_MAGIC) return;
    if (memcmp(msg.origin, s_my_mac, 6) == 0) return;     // own message came back

    if (msg.msg_type == MSG_TYPE_ANNOUNCE) {
        // Direct neighbour => received_ttl == INITIAL_TTL => hops == 1.
        uint8_t hops = (uint8_t)(INITIAL_TTL - msg.ttl + 1);
        int8_t  rssi = info->rx_ctrl->rssi;   // rx_ctrl is a pointer in esp_now_recv_info_t

        // IMPORTANT: update the table from EVERY copy first, so already_seen()
        // (which suppresses re-flooding) can never hide a better path from us.
        // For ANNOUNCE the text field carries the sender's user name.
        table_update(msg.origin, msg.text, msg.seq, hops, rssi);

        // relay-once: forward each (origin, seq) pair only a single time.
        if (already_seen(msg.origin, msg.seq)) return;
        if (msg.ttl > 0) {
            msg.ttl--;
            broadcast(&msg);
        }
        return;
    }

    // ---- plain chat DATA ----
    if (already_seen(msg.origin, msg.seq)) return;        // duplicate via another hop

    printf("[%02X:%02X:%02X:%02X:%02X:%02X] %s\n",
           msg.origin[0], msg.origin[1], msg.origin[2],
           msg.origin[3], msg.origin[4], msg.origin[5], msg.text);

    // relay further into the mesh
    if (msg.ttl > 0) {
        msg.ttl--;
        broadcast(&msg);
    }
}

// Periodically shout an ANNOUNCE so neighbours can discover us. The beacon is
// flooded just like chat: ttl = INITIAL_TTL, decremented on each relay.
static void announce_task(void *arg)
{
    chat_msg_t msg = { .magic = CHAT_MAGIC, .msg_type = MSG_TYPE_ANNOUNCE };
    memcpy(msg.origin, s_my_mac, 6);
    // ANNOUNCE payload is just our user name (kept short to stay well under 250B).
    snprintf(msg.text, sizeof(msg.text), "%s", USER_NAME);

    while (1) {
        msg.seq = next_seq();
        msg.ttl = INITIAL_TTL;
        broadcast(&msg);
        vTaskDelay(pdMS_TO_TICKS(ANNOUNCE_INTERVAL_MS));
    }
}

// Periodically expire nodes we haven't heard from in NODE_TIMEOUT_MS.
static void reaper_task(void *arg)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(REAPER_INTERVAL_MS));
        uint32_t now = now_ms();
        xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);
        for (int i = 0; i < MAX_NODES; i++) {
            if (s_nodes[i].in_use &&
                (now - s_nodes[i].last_seen_ms) > NODE_TIMEOUT_MS) {
                s_nodes[i].in_use = false;
            }
        }
        xSemaphoreGive(s_nodes_mtx);
    }
}

// Print the visible-node table to the console. Runs in the console task, never
// in the recv callback. We snapshot under the lock and do the slow USB I/O
// afterwards so on_recv / the reaper are not blocked by console output.
static void print_table(void)
{
    node_entry_t snap[MAX_NODES];
    uint32_t now;

    xSemaphoreTake(s_nodes_mtx, portMAX_DELAY);
    now = now_ms();
    memcpy(snap, s_nodes, sizeof(snap));
    xSemaphoreGive(s_nodes_mtx);

    printf("%-3s%-20s%-12s%-6s%-6s%s\n", "#", "MAC", "NAME", "RSSI", "HOPS", "AGE");
    int row = 0;
    for (int i = 0; i < MAX_NODES; i++) {
        node_entry_t *n = &snap[i];
        if (!n->in_use) continue;

        uint32_t age_s = (now - n->last_seen_ms) / 1000;

        char mac_str[18];
        snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X",
                 n->mac[0], n->mac[1], n->mac[2], n->mac[3], n->mac[4], n->mac[5]);

        char rssi_str[8];
        if (n->rssi_valid) snprintf(rssi_str, sizeof(rssi_str), "%d", n->rssi);
        else               snprintf(rssi_str, sizeof(rssi_str), "N/A");

        printf("%-3d%-20s%-12s%-6s%-6u%lus\n", row, mac_str, n->name, rssi_str, n->hops, (unsigned long)age_s);
        row++;
    }
    if (row == 0) printf("(no nodes seen yet)\n");
}

static void wifi_espnow_init(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
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
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE)); // don't sleep, or we miss packets

    ESP_ERROR_CHECK(esp_now_init());
    ESP_ERROR_CHECK(esp_now_register_recv_cb(on_recv));

    esp_now_peer_info_t peer = {
        .channel = WIFI_CHANNEL,
        .ifidx = WIFI_IF_STA,
        .encrypt = false,
    };
    memcpy(peer.peer_addr, BROADCAST_MAC, 6);
    ESP_ERROR_CHECK(esp_now_add_peer(&peer));

}

void app_main(void)
{
    vTaskDelay(1000 / portTICK_PERIOD_MS); // Wait for UART to initialize

    usb_serial_jtag_driver_config_t usj_config = {
        .tx_buffer_size = BUF_SIZE,
        .rx_buffer_size = BUF_SIZE,
    };
    ESP_ERROR_CHECK(usb_serial_jtag_driver_install(&usj_config));

    // Create the table mutex BEFORE ESP-NOW starts: on_recv may fire as soon as
    // the recv callback is registered inside wifi_espnow_init().
    s_nodes_mtx = xSemaphoreCreateMutex();
    ESP_ERROR_CHECK(s_nodes_mtx ? ESP_OK : ESP_ERR_NO_MEM);

    wifi_espnow_init();

    esp_wifi_get_mac(WIFI_IF_STA, s_my_mac);
    printf("Chat ready. My MAC: %02X:%02X:%02X:%02X:%02X:%02X, channel %d\n",
           s_my_mac[0], s_my_mac[1], s_my_mac[2],
           s_my_mac[3], s_my_mac[4], s_my_mac[5], WIFI_CHANNEL);

    uint32_t version = 0;
    ESP_ERROR_CHECK(esp_now_get_version(&version));
    printf("ESP-NOW version: %ld\n", version);

    // Start node discovery: announce ourselves and reap stale nodes.
    xTaskCreate(announce_task, "announce", 3072, NULL, 4, NULL);
    xTaskCreate(reaper_task,   "reaper",   2560, NULL, 3, NULL);

    printf("Type a message and press Enter. Type \"list\" to show the node table.\n");

    chat_msg_t msg = { .magic = CHAT_MAGIC, .msg_type = MSG_TYPE_DATA };
    memcpy(msg.origin, s_my_mac, 6);

    uint8_t chunk[64];
    size_t line_len = 0;

    while (1) {
        int n = usb_serial_jtag_read_bytes(chunk, sizeof(chunk), 20 / portTICK_PERIOD_MS);
        for (int i = 0; i < n; i++) {
            char c = (char)chunk[i];
            if (c == '\n' || c == '\r') {
                if (line_len == 0) continue;
                msg.text[line_len] = '\0';
                if (strcmp(msg.text, "list") == 0) {
                    print_table();            // console command, not sent over the air
                } else {
                    msg.seq = next_seq();
                    msg.ttl = MSG_TTL;
                    msg.msg_type = MSG_TYPE_DATA;
                    broadcast(&msg);
                    printf("[me] %s\n", msg.text);
                }
                line_len = 0;
            } else if (line_len < sizeof(msg.text) - 1) {
                msg.text[line_len++] = c;
            }
        }
    }
}
