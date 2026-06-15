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

#define BUF_SIZE        1024
#define WIFI_CHANNEL    1   // must be the same on all boards
#define MSG_TTL         5   // max hops through the mesh
#define DEDUP_SIZE      16  // remembered messages for flood suppression

// ESP-NOW payload is limited to 250 bytes total
typedef struct __attribute__((packed)) {
    uint16_t magic;     // filter out foreign ESP-NOW traffic
    uint8_t origin[6];  // MAC of the board that wrote the message
    uint16_t seq;       // per-origin message counter
    uint8_t ttl;
    char text[200];
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

    wifi_espnow_init();

    esp_wifi_get_mac(WIFI_IF_STA, s_my_mac);
    printf("Chat ready. My MAC: %02X:%02X:%02X:%02X:%02X:%02X, channel %d\n",
           s_my_mac[0], s_my_mac[1], s_my_mac[2],
           s_my_mac[3], s_my_mac[4], s_my_mac[5], WIFI_CHANNEL);
    printf("Type a message and press Enter:\n");

    chat_msg_t msg = { .magic = CHAT_MAGIC };
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
                msg.seq = ++s_my_seq;
                msg.ttl = MSG_TTL;
                broadcast(&msg);
                printf("[me] %s\n", msg.text);
                line_len = 0;
            } else if (line_len < sizeof(msg.text) - 1) {
                msg.text[line_len++] = c;
            }
        }
    }
}
