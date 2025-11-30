/************************************************************
 * DirtData Node (ESP32-C6, Arduino core)
 * ----------------------------------------------------------
 * - Captive-portal Setup Wizard (SoftAP + WebServer + DNS)
 * - One-shot run:
 *      * Power sensor rail
 *      * Init I2C, SCD4x (robust, avoid-NACK), ADS1115
 *      * WAIT for SCD4x data-ready (getDataReadyStatus +
 *        lowPowerWaitMs: delay vs light-sleep based on DEBUG)
 *      * Take ONE snapshot:
 *           - SCD4x (CO₂, T, RH)
 *           - ADS1115:
 *               AIN0 → 3.3V rail
 *               AIN1 → moisture node
 *               AIN2 → microbial node
 *               AIN3 → VBAT divider
 *           - ESP32 ADC:
 *               ESP_ADC_VBAT  → VBAT divider
 *               ESP_ADC_MOIST → moisture node
 *               ESP_ADC_SEN   → microbial node
 *           - DS18B20 soil temperature
 *      * Pack everything into SensorData_t
 *      * Connect Wi-Fi
 *      * Grab NTP timestamp (UTC ISO) while Wi-Fi is up
 *      * Build ArcGIS payload from SensorData_t and POST once
 *      * Log same SensorData_t as CSV row to /DirtData.csv on SD
 *      * Deep sleep for cfg.interval_min minutes
 *
 * - DEBUG flag:
 *      DEBUG=1 → Serial + delay() in waits
 *      DEBUG=0 → no Serial + light sleep in waits
 ************************************************************/

#include <Arduino.h>
#include <WiFi.h>
#include <Wire.h>
#include <WebServer.h>
#include <DNSServer.h>
#include <Preferences.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>

#include <SensirionI2cScd4x.h>
#include <Adafruit_ADS1X15.h>

#include <OneWireNg_CurrentPlatform.h>
#include <drivers/DSTherm.h>
#include <utils/Placeholder.h>

#include "esp_sleep.h"
#include "esp_log.h"

#include <SPI.h>
#include <SD.h>
#include <time.h>

// ================== DEBUG ==================
#define DEBUG 1
#if DEBUG
  #define LOG(...) do { Serial.printf(__VA_ARGS__); } while (0)
#else
  #define LOG(...) do {} while (0)
#endif

// ================== PINS ===================
// Analog
#define ESP_ADC_SEN    4   // microbial sensor node
#define ESP_ADC_VBAT   5   // VBAT divider node
#define ESP_ADC_MOIST  6   // moisture sensor node

// Sensor rails / I2C / 1-Wire / button
#define ESP_PWR_3V3    0   // SENSOR rail enable
#define ESP_GPS_3V3    1
#define ESP_SDA       10
#define ESP_SCL       11
#define ESP_TEMP       7
#define BOOT_BTN       9

// SD card (SPI)
#define SD_CD         20   // Card Detect (active low)
#define SD_MOSI       23
#define SD_MISO       22
#define SD_SCK        19
#define SD_CS         21

// ============== CONSTANTS ==================
const float R_KNOWN             = 1000.0f;   // to 3V3
const float DIVIDER_RATIO       = 2.0f;      // VBAT divider
const float BATTERY_VOLTAGE_MAX = 4.5f;
const float BATTERY_VOLTAGE_MIN = 2.0f;
const float MOISTURE_MV_DRY     = 3300.0f;
const float MOISTURE_MV_WET     = 1000.0f;

// I2C
#define I2C_SPEED_HZ 100000UL
const uint8_t SCD4X_ADDR = 0x62;
const uint8_t ADS_ADDR   = 0x48;

// ============== CLOUD (ArcGIS) =============
const char *ARC_ENDPOINT = "https://gis.dirtdata.ai/arcgis/rest/services/dirtdata/Dirt_Data_Nodes_POC/FeatureServer/0/addFeatures";
const char *ARC_REFERER  = "https://sensor.dirtdata.ai";
const char *ARC_API_KEY  = "AAPTg-FeepoS1ejSr4QBtPc02Zp-UeIfxKMnp96qOukL9MTt1OknOyChtkEQysGxYXi61t4XXUVHo7KQJEdrOxPhe3DSuFOgb46JJ8fUniAd0oPlBpC8hjn-V0kCDuUKtfqP0bon1bLJM5briX_nXmRgiogHzqhPOFLgM8k7hr7VxWRlq_C7GL7XfMmJt959xRSJ1XovVDrjkCd5J30Qhc_vFAayyoZyPmhCDSPTSkvQJnY.AT1_RjRfFhTZ";

// ================== CONFIG =================
Preferences prefs;

struct Config {
  String nickname;
  String lat;
  String lon;
  uint32_t interval_min;
  String wifi_ssid;
  String wifi_pass;
} cfg;

// ================== SNAPSHOTS & PAYLOAD =================
struct AnalogSnapshot_t {
  float vbat_v;     // battery voltage (V)
  float vbat_pct;   // battery percent (%)
  float moist_v;    // moisture node (V)
  float moist_pct;  // moisture (%)
  float micro_v;    // microbial node (V)
  float r_ohms;     // derived resistance (Ω)
};

struct SensorData_t {
  int32_t nodeId;
  char    timestampUtc[32];   // "YYYY-MM-DDTHH:MM:SSZ"

  float latitude;
  float longitude;

  // SCD4x
  float co2PPM;
  float airTempC;
  float airHumidity;

  // Soil temp
  float soilTempC;

  // Battery
  float batteryVoltage;      // V (preferred ADS)
  float batteryPercent;      // %

  // Moisture
  float soilMoisture_mV;     // mV at node
  float soilMoisturePercent; // %

  // Microbial
  float microVoltage;        // V at node
  float resistance;          // Ω

  // ADS raw
  float ads_v3v3;
  float ads_moist_v;
  float ads_micro_v;
  float ads_vbat_div_v;

  // ESP raw (mV)
  float esp_vbat_mV;
  float esp_moist_mV;
  float esp_micro_mV;

  char  nodeName[32];
};

// ================== GLOBALS =================
// SCD4x
SensirionI2cScd4x scd4x;
bool  g_scd_present = false;
bool  g_scd_running = false;
float g_scd_co2  = NAN;
float g_scd_temp = NAN;
float g_scd_rh   = NAN;

// ADS1115
Adafruit_ADS1115 ads;
bool  g_ads_present = false;
const float ADS_FSR_V = 4.096f;
float g_ads_v3v3 = NAN;  // for ESP resistance calc

// 1-Wire temp
OneWireNg_CurrentPlatform oneWire(ESP_TEMP, false);
DSTherm tempSensor(oneWire);

// Wizard / server
WebServer server(80);
DNSServer dns;
const byte DNS_PORT = 53;

// SD
bool g_sd_ok = false;

// ================== DEBUG SLEEP HELPER =================
void lowPowerWaitMs(uint32_t ms) {
#if DEBUG
  delay(ms);
#else
  uint64_t us = (uint64_t)ms * 1000ULL;
  esp_sleep_enable_timer_wakeup(us);
  Serial.flush();
  esp_light_sleep_start();
#endif
}

// ================== UTILS ===================
static inline bool validF(float v) { return isfinite(v); }
static inline float clampF(float v, float lo, float hi) {
  if (!validF(v)) return NAN;
  if (v < lo) v = lo;
  if (v > hi) v = hi;
  return v;
}

float batteryPercentFromVoltage(float v) {
  if (!validF(v)) return NAN;
  float pct = 100.0f * (v - BATTERY_VOLTAGE_MIN) /
                         (BATTERY_VOLTAGE_MAX - BATTERY_VOLTAGE_MIN);
  if (pct < 0.0f)  pct = 0.0f;
  if (pct > 100.0f) pct = 100.0f;
  return pct;
}

float moisturePercentFromMv(float mv) {
  if (!validF(mv)) return NAN;
  float pct = 100.0f * (MOISTURE_MV_DRY - mv) /
                         (MOISTURE_MV_DRY - MOISTURE_MV_WET);
  if (pct < 0.0f)  pct = 0.0f;
  if (pct > 100.0f) pct = 100.0f;
  return pct;
}

// 3V3 -- R_KNOWN --o-- R_sensor -- GND
float computeSensorResistance(float v_3v3, float v_sen) {
  if (!validF(v_3v3) || !validF(v_sen)) return NAN;
  if (v_sen <= 0.0f || v_sen >= v_3v3) return NAN;
  return R_KNOWN * (v_sen / (v_3v3 - v_sen));
}

// MAC → node_id
int32_t GetNodeIdFromMac() {
  uint64_t mac = ESP.getEfuseMac();
  uint32_t h = 2166136261u;
  for (int i = 0; i < 6; ++i) {
    uint8_t b = (mac >> (8 * i)) & 0xFF;
    h ^= b; h *= 16777619u;
  }
  return (int32_t)(h & 0x7FFFFFFF);
}

static String macLast4() {
  uint8_t mac[6]; WiFi.macAddress(mac);
  char buf[5]; snprintf(buf, sizeof(buf), "%02X%02X", mac[4], mac[5]);
  return String(buf);
}
static String macStr() {
  uint8_t mac[6]; WiFi.macAddress(mac);
  char buf[18];
  snprintf(buf, sizeof(buf), "%02X:%02X:%02X:%02X:%02X:%02X",
           mac[0],mac[1],mac[2],mac[3],mac[4],mac[5]);
  return String(buf);
}

// ================== HTML/WIZARD =================
static String fmtCoord(double v, int decimals = 6) {
  char buf[32]; dtostrf(v, 0, decimals, buf); return String(buf);
}

String iosHead(const char* title) {
  String css; css.reserve(2600);
  css += F(
"<!doctype html><html lang=\"en\"><head>\n"
"<meta charset=\"utf-8\">\n"
"<meta name=\"viewport\" content=\"viewport-fit=cover,width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no\">\n"
"<title>");
  css += title;
  css += F(
"</title>\n"
"<style>\n"
":root{--blue:#0A84FF;--text:#0A0A0A;--muted:#6E6E73;--bg:#fff;--card:#fff;--shadow:0 6px 24px rgba(0,0,0,.08);--radius:16px}\n"
"*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}\n"
"html,body{margin:0;padding:0;background:var(--bg);color:var(--text);font:16px -apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,Inter,Helvetica,Arial,sans-serif}\n"
".safe{padding:env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left)}\n"
".wrap{min-height:100dvh;display:flex;align-items:center;justify-content:center;padding:24px}\n"
".card{max-width:720px;width:100%;background:var(--card);border-radius:var(--radius);box-shadow:var(--shadow);padding:28px}\n"
".title{font-size:28px;font-weight:700;letter-spacing:-.02em;margin:4px 0 8px}\n"
".subtitle{font-size:15px;color:#6E6E73;margin:0 0 18px}\n"
".brand{display:flex;align-items:center;gap:10px;margin-bottom:8px}\n"
".chip{display:inline-block;padding:6px 10px;border-radius:999px;background:#E6F0FF;color:#003EAA;font-weight:600;font-size:12px;letter-spacing:.02em}\n"
".hero{margin:16px 0 20px;line-height:1.6}\n"
".row{display:flex;flex-direction:column;gap:10px;margin:12px 0}\n"
".label{font-size:13px;color:#6E6E73;margin-bottom:2px}\n"
".input,.button{width:100%;border-radius:14px;padding:14px 16px;font-size:16px;border:1px solid #E5E5EA;background:#fff;outline:none}\n"
".input:focus{border-color:#0A84FF;box-shadow:0 0 0 3px rgba(10,132,255,.15)}\n"
".actions{display:flex;gap:12px;flex-wrap:wrap;margin-top:18px}\n"
".btn{appearance:none;border:0;border-radius:14px;padding:14px 18px;font-weight:700;font-size:16px;letter-spacing:.2px;text-decoration:none;display:inline-flex;align-items:center;gap:8px;cursor:pointer}\n"
".btn-primary{background:var(--blue);color:#fff;box-shadow:0 4px 12px rgba(10,132,255,.35)}\n"
".btn-secondary{background:#EEF2FF;color:#103D98}\n"
".btn-link{background:transparent;color:#0A84FF;padding:10px 0}\n"
".small{font-size:13px;color:#6E6E73}\n"
".meta{margin-top:14px;font-size:12px;color:var(--muted)}\n"
".footer{margin-top:14px;font-size:12px;color:var(--muted)}\n"
".badge{display:inline-block;background:#FFF7E6;color:#8A5200;border:1px solid #FFE1A6;border-radius:999px;padding:4px 8px;font-size:12px}\n"
".err{color:#B00020;font-size:13px;margin-top:6px}\n"
".ok{color:#007A0A;font-size:13px;margin-top:6px}\n"
"</style>\n"
"</head><body><div class=\"safe wrap\"><main class=\"card\">\n"
"<div class=\"brand\">\n"
"  <svg width=\"28\" height=\"28\" viewBox=\"0 0 24 24\" fill=\"none\" aria-hidden=\"true\">\n"
"    <rect x=\"3\" y=\"3\" width=\"18\" height=\"18\" rx=\"6\" fill=\"#0A84FF\"/>\n"
"    <path d=\"M8.2 12.4c2.1-2.6 5.5-2.6 7.6 0\" stroke=\"#fff\" stroke-width=\"2\" stroke-linecap=\"round\"/>\n"
"    <circle cx=\"8.8\" cy=\"13.8\" r=\"1.2\" fill=\"#fff\"/>\n"
"    <circle cx=\"15.2\" cy=\"13.8\" r=\"1.2\" fill=\"#fff\"/>\n"
"  </svg>\n"
"  <span class=\"chip\">DirtData</span>\n"
"</div>\n");
  return css;
}

String iosFoot(const String& meta) {
  String f; f.reserve(600);
  f += F("<div class=\"meta\">");
  f += meta;
  f += F("</div><div class=\"footer\">© ");
  f += String(__DATE__);
  f += F(" DirtData • All rights reserved</div></main></div></body></html>");
  return f;
}

String pageWelcome() {
  String html; html.reserve(6000);
  html += iosHead("DirtData • Welcome");
  html += F(
"<h1 class=\"title\">Welcome — thanks for choosing DirtData!</h1>\n"
"<p class=\"subtitle\">Setup • Page 1 of 5</p>\n"
"<p class=\"hero\">This wizard configures your node name, location, upload interval, and Wi-Fi connection.</p>\n"
"<div class=\"actions\"><a class=\"btn btn-primary\" href=\"/device-name\">Get Started</a></div>\n");
  String meta = "SSID: <b>DirtData-Setup-" + macLast4() + "</b> • Device ID: <b>" + macStr() + "</b>";
  html += iosFoot(meta);
  return html;
}

String pageDeviceName() {
  String html; html.reserve(7000);
  html += iosHead("DirtData • Device Name & ID");
  html += F(
"<a class=\"btn btn-link\" href=\"/\">← Back</a>\n"
"<h1 class=\"title\">Name your device</h1>\n"
"<p class=\"subtitle\">Setup • Page 2 of 5</p>\n"
"<form method=\"POST\" action=\"/device-name\" autocomplete=\"on\">\n"
"  <fieldset class=\"row\">\n"
"    <label class=\"label\" for=\"nickname\">Device Nickname</label>\n"
"    <input class=\"input\" id=\"nickname\" name=\"nickname\" type=\"text\" minlength=\"1\" maxlength=\"40\"\n"
"           placeholder=\"e.g., North Plot Node\" value=\"");
  html += cfg.nickname;
  html += F("\" required>\n"
"    <span class=\"small\">You can change this later.</span>\n"
"  </fieldset>\n"
"  <div class=\"actions\">\n"
"    <button class=\"btn btn-secondary\" type=\"button\" onclick=\"history.back()\">Back</button>\n"
"    <button class=\"btn btn-primary\" type=\"submit\">Continue</button>\n"
"  </div>\n"
"</form>\n");
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageDeviceLocation() {
  String html; html.reserve(12000);
  html += iosHead("DirtData • Device Location");
  html += F(
"<a class=\"btn btn-link\" href=\"/device-name\">← Back</a>\n"
"<h1 class=\"title\">Device location</h1>\n"
"<p class=\"subtitle\">Setup • Page 3 of 5</p>\n"
"<div id=\"gpsWarn\" class=\"small\" style=\"display:none\"><span class=\"badge\">Note</span> Your browser may block GPS on captive/HTTP pages. If the button does nothing, enter coordinates manually.</div>\n"
"<div class=\"actions\"><button class=\"btn btn-secondary\" type=\"button\" id=\"useGPS\">Use phone GPS</button></div>\n"
"<form id=\"locform\" method=\"POST\" action=\"/device-location\" autocomplete=\"on\" novalidate>\n"
"  <fieldset class=\"row\">\n"
"    <label class=\"label\" for=\"lat\">Latitude (−90..90)</label>\n"
"    <input class=\"input\" id=\"lat\" name=\"lat\" type=\"text\" inputmode=\"text\" placeholder=\"40.014990\" value=\"");
  html += cfg.lat;
  html += F("\" required>\n"
"    <div id=\"latErr\" class=\"err\" style=\"display:none\">Enter a latitude between −90 and 90.</div>\n"
"  </fieldset>\n"
"  <fieldset class=\"row\">\n"
"    <label class=\"label\" for=\"lon\">Longitude (−180..180)</label>\n"
"    <input class=\"input\" id=\"lon\" name=\"lon\" type=\"text\" inputmode=\"text\" placeholder=\"-105.270550\" value=\"");
  html += cfg.lon;
  html += F("\" required>\n"
"    <div id=\"lonErr\" class=\"err\" style=\"display:none\">Enter a longitude between −180 and 180.</div>\n"
"  </fieldset>\n"
"  <div class=\"actions\">\n"
"    <button class=\"btn btn-secondary\" type=\"button\" onclick=\"history.back()\">Back</button>\n"
"    <button class=\"btn btn-primary\" type=\"submit\">Continue</button>\n"
"  </div>\n"
"</form>\n"
"<script>\n"
"(function(){ if(!window.isSecureContext){ document.getElementById('gpsWarn').style.display='block'; } })();\n"
"(function(){\n"
"  const btn=document.getElementById('useGPS'); const lat=document.getElementById('lat'); const lon=document.getElementById('lon');\n"
"  if(!btn) return;\n"
"  btn.addEventListener('click',function(){\n"
"    if(!('geolocation' in navigator)){ alert('Geolocation not supported.'); return; }\n"
"    const old=btn.textContent; btn.disabled=true; btn.textContent='Getting location…';\n"
"    navigator.geolocation.getCurrentPosition(function(pos){\n"
"      lat.value=pos.coords.latitude.toFixed(6);\n"
"      lon.value=pos.coords.longitude.toFixed(6);\n"
"      btn.textContent=old; btn.disabled=false;\n"
"    },function(err){\n"
"      alert('Could not get GPS; please enter manually.');\n"
"      btn.textContent=old; btn.disabled=false;\n"
"    },{enableHighAccuracy:true,timeout:15000,maximumAge:0});\n"
"  });\n"
"})();\n"
"(function(){\n"
"  const form=document.getElementById('locform'), lat=document.getElementById('lat'), lon=document.getElementById('lon');\n"
"  const latErr=document.getElementById('latErr'), lonErr=document.getElementById('lonErr');\n"
"  function vLat(v){const x=parseFloat(v);return isFinite(x)&&x>=-90&&x<=90;}\n"
"  function vLon(v){const x=parseFloat(v);return isFinite(x)&&x>=-180&&x<=180;}\n"
"  form.addEventListener('submit',function(e){\n"
"    let ok=true; if(!vLat(lat.value)){latErr.style.display='block';ok=false;} else latErr.style.display='none';\n"
"    if(!vLon(lon.value)){lonErr.style.display='block';ok=false;} else lonErr.style.display='none';\n"
"    if(!ok)e.preventDefault();\n"
"  });\n"
"})();\n"
"</script>\n");
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageAdvancedOptions() {
  String html; html.reserve(8000);
  html += iosHead("DirtData • Advanced Options");
  html += F(
"<a class=\"btn btn-link\" href=\"/device-location\">← Back</a>\n"
"<h1 class=\"title\">Advanced options</h1>\n"
"<p class=\"subtitle\">Setup • Page 4 of 5</p>\n"
"<form method=\"POST\" action=\"/advanced-options\" autocomplete=\"on\">\n"
"  <fieldset class=\"row\">\n"
"    <label class=\"label\" for=\"interval\">Interval (minutes)</label>\n"
"    <input class=\"input\" id=\"interval\" name=\"interval\" type=\"number\" inputmode=\"numeric\" step=\"1\" min=\"1\" max=\"10080\"\n"
"           placeholder=\"30\" value=\"");
  html += String(cfg.interval_min);
  html += F("\" required>\n"
"    <span class=\"small\">More frequent readings reduce battery life.</span>\n"
"  </fieldset>\n"
"  <div class=\"actions\">\n"
"    <button class=\"btn btn-secondary\" type=\"button\" onclick=\"history.back()\">Back</button>\n"
"    <button class=\"btn btn-primary\" type=\"submit\">Continue</button>\n"
"  </div>\n"
"</form>\n");
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageWifiCreds(const String& statusMsg = "", bool ok=false) {
  String html; html.reserve(9000);
  html += iosHead("DirtData • Wi-Fi");
  html += F(
"<a class=\"btn btn-link\" href=\"/advanced-options\">← Back</a>\n"
"<h1 class=\"title\">Wi-Fi credentials</h1>\n"
"<p class=\"subtitle\">Setup • Page 5 of 5</p>\n"
"<form method=\"POST\" action=\"/wifi\" autocomplete=\"on\">\n"
"  <fieldset class=\"row\">\n"
"    <label class=\"label\" for=\"ssid\">SSID</label>\n"
"    <input class=\"input\" id=\"ssid\" name=\"ssid\" type=\"text\" minlength=\"1\" maxlength=\"64\"\n"
"           placeholder=\"Your Wi-Fi name\" value=\"");
  html += cfg.wifi_ssid;
  html += F("\" required>\n"
"  </fieldset>\n"
"  <fieldset class=\"row\">\n"
"    <label class=\"label\" for=\"pass\">Password</label>\n"
"    <input class=\"input\" id=\"pass\" name=\"pass\" type=\"password\" maxlength=\"64\"\n"
"           placeholder=\"Your Wi-Fi password\" value=\"");
  html += cfg.wifi_pass;
  html += F("\">\n"
"  </fieldset>\n"
"  <div class=\"actions\">\n"
"    <button class=\"btn btn-secondary\" type=\"button\" onclick=\"history.back()\">Back</button>\n"
"    <button class=\"btn btn-primary\" type=\"submit\">Test & Save</button>\n"
"  </div>\n"
"</form>\n");
  if (statusMsg.length()) {
    html += String("<div class=\"") + (ok?"ok":"err") + "\">" + statusMsg + "</div>";
  }
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageSaved() {
  String html; html.reserve(6000);
  html += iosHead("DirtData • Setup Saved");
  html += F(
"<h1 class=\"title\">Setup saved</h1>\n"
"<p class=\"subtitle\">You're good to go. You may close this page.</p>\n"
"<div class=\"hero\">\n");
  html += "Nickname: <b>" + (cfg.nickname.length()?cfg.nickname:String("(none)")) + "</b><br>";
  html += "Location: <b>" + (cfg.lat.length()?cfg.lat:String("(lat?)")) + ", " +
          (cfg.lon.length()?cfg.lon:String("(lon?)")) + "</b><br>";
  html += "Interval: <b>" + String(cfg.interval_min) + " min</b><br>";
  html += "Connection: <b>Direct Wi-Fi</b>";
  html += F(
"</div>\n"
"<div class=\"actions\"><a class=\"btn btn-primary\" href=\"/\">Finish</a></div>\n");
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

// HTTP handlers
void handleRoot() { server.send(200, "text/html; charset=utf-8", pageWelcome()); }
void handleGetDeviceName() { server.send(200, "text/html; charset=utf-8", pageDeviceName()); }
void handlePostDeviceName() {
  if (server.hasArg("nickname")) {
    String v = server.arg("nickname");
    if (v.length() > 40) v.remove(40);
    cfg.nickname = v;
    prefs.putString("nickname", cfg.nickname);
  }
  server.sendHeader("Location", "/device-location", true);
  server.send(303, "text/plain", "");
}

static String clampCoordStr(const String& s, float lo, float hi) {
  double x = atof(s.c_str());
  if (isnan(x) || isinf(x)) return String("");
  if (x < lo) x = lo;
  if (x > hi) x = hi;
  return fmtCoord(x, 6);
}
void handleGetDeviceLocation() { server.send(200, "text/html; charset=utf-8", pageDeviceLocation()); }
void handlePostDeviceLocation() {
  if (server.hasArg("lat")) { cfg.lat = clampCoordStr(server.arg("lat"), -90, 90); prefs.putString("lat", cfg.lat); }
  if (server.hasArg("lon")) { cfg.lon = clampCoordStr(server.arg("lon"), -180, 180); prefs.putString("lon", cfg.lon); }
  server.sendHeader("Location", "/advanced-options", true);
  server.send(303, "text/plain", "");
}

void handleGetAdvancedOptions() { server.send(200, "text/html; charset=utf-8", pageAdvancedOptions()); }
void handlePostAdvancedOptions() {
  if (server.hasArg("interval")) {
    long v = server.arg("interval").toInt();
    if (v < 1) v = 1;
    if (v > 10080) v = 10080;
    cfg.interval_min = (uint32_t)v;
    prefs.putUInt("interval_min", cfg.interval_min);
  }
  server.sendHeader("Location", "/wifi", true);
  server.send(303, "text/plain", "");
}

void handleGetWifi() { server.send(200, "text/html; charset=utf-8", pageWifiCreds()); }
bool testStaJoin(const String& ssid, const String& pass, uint32_t timeoutMs=15000) {
  WiFi.mode(WIFI_AP_STA);
  WiFi.begin(ssid.c_str(), pass.c_str());
  uint32_t t0 = millis();
  while (WiFi.status()!=WL_CONNECTED && millis()-t0<timeoutMs) delay(200);
  bool ok = (WiFi.status()==WL_CONNECTED);
  WiFi.disconnect(true, true);
  WiFi.mode(WIFI_AP);
  return ok;
}
void handlePostWifi() {
  String ssid = server.hasArg("ssid") ? server.arg("ssid") : "";
  String pass = server.hasArg("pass") ? server.arg("pass") : "";
  ssid.trim(); pass.trim();

  if (!ssid.length()) {
    server.send(200, "text/html; charset=utf-8", pageWifiCreds("SSID is required.", false));
    return;
  }

  bool ok = testStaJoin(ssid, pass);
  if (ok) {
    cfg.wifi_ssid = ssid;
    cfg.wifi_pass = pass;
    prefs.putString("wifi_ssid", cfg.wifi_ssid);
    prefs.putString("wifi_pass", cfg.wifi_pass);
    server.send(200, "text/html; charset=utf-8", pageSaved());
  } else {
    server.send(200, "text/html; charset=utf-8",
                pageWifiCreds("Could not join that Wi-Fi. Check SSID/password and try again.", false));
  }
}

// Captive portal helpers
void handleRedirectToRoot() { server.sendHeader("Location", "/", true); server.send(302, "text/plain", ""); }
void handleAppleCaptive()   { server.sendHeader("Location", "/", true); server.send(200, "text/html", "<meta http-equiv='refresh' content='0; url=/'/>"); }
void handleAndroidCaptive() { server.sendHeader("Location", "/", true); server.send(200, "text/html", "<meta http-equiv='refresh' content='0; url=/'/>"); }
void handleWindowsCaptive() { server.sendHeader("Location", "/", true); server.send(200, "text/html", "<meta http-equiv='refresh' content='0; url=/'/>"); }

void startAP() {
  WiFi.setSleep(false);
  WiFi.mode(WIFI_AP);
  String ssid = "DirtData-Setup-" + macLast4();
  WiFi.softAP(ssid.c_str(), nullptr, 6, 0, 4);
}

void startDNS() {
  IPAddress apIP = WiFi.softAPIP();
  dns.start(DNS_PORT, "*", apIP);
}

void startHTTP() {
  server.on("/", HTTP_GET, handleRoot);
  server.on("/device-name", HTTP_GET, handleGetDeviceName);
  server.on("/device-name", HTTP_POST, handlePostDeviceName);
  server.on("/device-location", HTTP_GET, handleGetDeviceLocation);
  server.on("/device-location", HTTP_POST, handlePostDeviceLocation);
  server.on("/advanced-options", HTTP_GET, handleGetAdvancedOptions);
  server.on("/advanced-options", HTTP_POST, handlePostAdvancedOptions);
  server.on("/wifi", HTTP_GET, handleGetWifi);
  server.on("/wifi", HTTP_POST, handlePostWifi);

  server.on("/hotspot-detect.html", HTTP_GET, handleAppleCaptive);
  server.on("/generate_204", HTTP_GET, handleAndroidCaptive);
  server.on("/gen_204", HTTP_GET, handleAndroidCaptive);
  server.on("/ncsi.txt", HTTP_GET, handleWindowsCaptive);
  server.on("/connecttest.txt", HTTP_GET, handleWindowsCaptive);
  server.on("/fwlink", HTTP_GET, handleWindowsCaptive);

  server.onNotFound(handleRedirectToRoot);
  server.begin();
}

void loadPrefs() {
  prefs.begin("dirtdata", false);
  cfg.nickname     = prefs.getString("nickname", "");
  cfg.lat          = prefs.getString("lat", "");
  cfg.lon          = prefs.getString("lon", "");
  cfg.interval_min = prefs.getUInt("interval_min", 30);
  cfg.wifi_ssid    = prefs.getString("wifi_ssid", "");
  cfg.wifi_pass    = prefs.getString("wifi_pass", "");
}

// ================== JSON HELPERS =================
static inline void setOrNullF(JsonObject obj, const char* key, float value, bool ok) {
  if (ok) obj[key] = value; else obj[key] = nullptr;
}
static inline void setOrNullI(JsonObject obj, const char* key, int value, bool ok) {
  if (ok) obj[key] = value; else obj[key] = nullptr;
}
static inline bool validCoord(float lat, float lon) {
  return validF(lat) && validF(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

static String urlEncode(const String& s) {
  String out; out.reserve(s.length()*3);
  for (size_t i=0;i<s.length();i++) {
    char c=s[i];
    bool unres = (c>='A'&&c<='Z')||(c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-'||c=='_'||c=='.'||c=='~';
    if (unres) out+=c;
    else { char b[4]; sprintf(b,"%%%02X",(uint8_t)c); out+=b; }
  }
  return out;
}

// ================== I2C / SCD4x =================
static bool i2cProbe(uint8_t addr) {
  Wire.beginTransmission(addr);
  return (Wire.endTransmission() == 0);
}

// Avoid-NACK bring-up: probe → stop → reinit → start
static bool scdInitRobust() {
  Wire.begin(ESP_SDA, ESP_SCL);
  Wire.setClock(I2C_SPEED_HZ);
  delay(10);

  g_scd_present = i2cProbe(SCD4X_ADDR);
  if (!g_scd_present) {
    LOG("[ERROR] [SCD4x] Not found at 0x%02X\n", SCD4X_ADDR);
    return false;
  }

  scd4x.begin(Wire, SCD4X_ADDR);
  delay(5);

  int16_t e = scd4x.stopPeriodicMeasurement();
  if (e) LOG("[ERROR] [SCD4x] stopPeriodicMeasurement=0x%04X\n", (uint16_t)e);
  delay(2);

  e = scd4x.reinit();
  if (e) LOG("[ERROR] [SCD4x] reinit=0x%04X\n", (uint16_t)e);
  delay(20);

  e = scd4x.startPeriodicMeasurement();
  if (!e) {
    g_scd_running = true;
    return true;
  }
  LOG("[ERROR] [SCD4x] startPeriodicMeasurement=0x%04X\n", (uint16_t)e);
  return false;
}

// Wait for data-ready using getDataReadyStatus, then read once
static bool scdWaitAndRead(uint32_t timeout_ms = 6500) {
  if (!g_scd_present || !g_scd_running) return false;
  uint32_t t0 = millis();
  while (millis() - t0 < timeout_ms) {
    bool ready = false;
    int16_t e = scd4x.getDataReadyStatus(ready);
    if (e) {
      LOG("[ERROR] [SCD4x] getDataReadyStatus=0x%04X\n", (uint16_t)e);
      return false;
    }
    if (ready) {
      uint16_t co2 = 0;
      float t = NAN, rh = NAN;
      e = scd4x.readMeasurement(co2, t, rh);
      if (!e && co2 != 0) {
        g_scd_co2  = co2;
        g_scd_temp = t;
        g_scd_rh   = rh;
        LOG("[SCD4x] CO₂=%.1f ppm | T=%.2f °C | RH=%.2f %%\n", g_scd_co2, g_scd_temp, g_scd_rh);
        return true;
      }
      LOG("[ERROR] [SCD4x] readMeasurement=0x%04X (co2=%u)\n", (uint16_t)e, co2);
      return false;
    }
    lowPowerWaitMs(250);  // DEBUG: delay, PROD: light sleep
  }
  LOG("[ERROR] [SCD4x] Timeout waiting for first sample\n");
  return false;
}

static void scdStop() {
  if (!g_scd_present) return;
  int16_t e = scd4x.stopPeriodicMeasurement();
  if (e) LOG("[ERROR] [SCD4x] stop=0x%04X\n", (uint16_t)e);
}

// ================== ADS1115 =================
bool adsInit() {
  if (!ads.begin(ADS_ADDR)) {
    LOG("[ERROR] [ADS1115] begin(0x%02X) failed\n", ADS_ADDR);
    return false;
  }
  ads.setGain(GAIN_ONE);  // ±4.096V
  return true;
}

float adsReadVoltage(uint8_t channel) {
  if (!g_ads_present) return NAN;
  int16_t raw = ads.readADC_SingleEnded(channel);
  float v = (float)raw * (ADS_FSR_V / 32768.0f);
  if (v < 0.0f) v = 0.0f;
  return v;
}

AnalogSnapshot_t readAdsSnapshot(float &ads_v3v3, float &ads_vbat_div) {
  AnalogSnapshot_t a{};
  if (!g_ads_present) {
    a.vbat_v = a.vbat_pct = a.moist_v = a.moist_pct = a.micro_v = a.r_ohms = NAN;
    ads_v3v3    = NAN;
    ads_vbat_div= NAN;
    return a;
  }

  float v_3v3   = adsReadVoltage(0);
  float v_moist = adsReadVoltage(1);
  float v_micro = adsReadVoltage(2);
  float v_div   = adsReadVoltage(3);

  g_ads_v3v3   = v_3v3;
  ads_v3v3     = v_3v3;
  ads_vbat_div = v_div;

  float v_bat   = validF(v_div) ? v_div * DIVIDER_RATIO : NAN;
  float bat_pct = batteryPercentFromVoltage(v_bat);

  float moist_mv   = validF(v_moist) ? v_moist * 1000.0f : NAN;
  float moist_pct  = moisturePercentFromMv(moist_mv);

  float r_micro = computeSensorResistance(v_3v3, v_micro);
  float micro_mv = validF(v_micro) ? v_micro * 1000.0f : NAN;

  a.vbat_v    = v_bat;
  a.vbat_pct  = bat_pct;
  a.moist_v   = v_moist;
  a.moist_pct = moist_pct;
  a.micro_v   = v_micro;
  a.r_ohms    = r_micro;

  LOG("[ADS1115] VBAT=%.3f V | BAT=%.1f %% | Moist=%.1f mV | MoistPct=%.1f %% | Micro=%.1f mV | R=%.1f Ω\n",
      v_bat,
      bat_pct,
      moist_mv,
      moist_pct,
      micro_mv,
      r_micro);

  return a;
}

// ================== ESP ADC =================
float espReadAvgMilliVolts(int pin, int samples = 10) {
  uint32_t sum = 0;
  for (int i=0;i<samples;i++) sum += analogReadMilliVolts(pin);
  return (float)sum / (float)samples;
}

AnalogSnapshot_t readEspSnapshot(float &esp_vbat_mV, float &esp_moist_mV, float &esp_micro_mV) {
  AnalogSnapshot_t a{};

  if (!validF(g_ads_v3v3) || g_ads_v3v3 <= 0.0f) {
    LOG("[ERROR] [ESP-ADC] Skipping (ADS 3V3 reference invalid)\n");
    a.vbat_v = a.vbat_pct = a.moist_v = a.moist_pct = a.micro_v = a.r_ohms = NAN;
    esp_vbat_mV = esp_moist_mV = esp_micro_mV = NAN;
    return a;
  }

  float vbat_mV   = espReadAvgMilliVolts(ESP_ADC_VBAT);
  float vbat_V    = (vbat_mV / 1000.0f) * DIVIDER_RATIO;
  float bat_pct   = batteryPercentFromVoltage(vbat_V);

  float moist_mV  = espReadAvgMilliVolts(ESP_ADC_MOIST);
  float moist_pct = moisturePercentFromMv(moist_mV);

  float micro_mV  = espReadAvgMilliVolts(ESP_ADC_SEN);
  float micro_V   = micro_mV / 1000.0f;
  float r_micro   = computeSensorResistance(g_ads_v3v3, micro_V);

  a.vbat_v    = vbat_V;
  a.vbat_pct  = bat_pct;
  a.moist_v   = moist_mV / 1000.0f;
  a.moist_pct = moist_pct;
  a.micro_v   = micro_V;
  a.r_ohms    = r_micro;

  esp_vbat_mV  = vbat_mV;
  esp_moist_mV = moist_mV;
  esp_micro_mV = micro_mV;

  LOG("[ESP-ADC] VBAT=%.3f V | BAT=%.1f %% | Moist=%.1f mV | MoistPct=%.1f %% | Micro=%.1f mV | R=%.1f Ω\n",
      vbat_V,
      bat_pct,
      moist_mV,
      moist_pct,
      micro_mV,
      r_micro);

  return a;
}

// ================== DS18B20 SOIL TEMP =================
float getSoilTempC() {
  Placeholder<DSTherm::Scratchpad> scrpd;
  tempSensor.convertTempAll(94, false); // 9-bit
  for (const auto& id : oneWire) {
    if (DSTherm::getFamilyName(id)) {
      if (tempSensor.readScratchpad(id, scrpd) == OneWireNg::EC_SUCCESS) {
        float t = scrpd->getTemp2()/16.0f;
        LOG("[TEMP] soilTempC=%.2f °C\n", t);
        return t;
      }
    }
  }
  LOG("[ERROR] [TEMP] No valid DS18B20 reading\n");
  return NAN;
}

// ================== BUILD SENSOR DATA FROM SNAPSHOTS =================
void buildSensorDataFromSnapshots(SensorData_t &d,
                                  const AnalogSnapshot_t &adsSnap,
                                  const AnalogSnapshot_t &espSnap,
                                  float ads_v3v3,
                                  float ads_vbat_div,
                                  float esp_vbat_mV,
                                  float esp_moist_mV,
                                  float esp_micro_mV,
                                  float soilTempC) {
  memset(&d, 0, sizeof(d));

  d.nodeId = GetNodeIdFromMac();
  d.timestampUtc[0] = '\0'; // filled later after NTP

  d.latitude  = cfg.lat.length()? atof(cfg.lat.c_str()) : NAN;
  d.longitude = cfg.lon.length()? atof(cfg.lon.c_str()) : NAN;

  // SCD4x (already read)
  d.co2PPM      = g_scd_co2;
  d.airTempC    = g_scd_temp;
  d.airHumidity = g_scd_rh;

  // Soil temp
  d.soilTempC   = soilTempC;

  // Preferred battery & moisture & resistance from ADS, fallback to ESP
  bool ads_bat_valid   = validF(adsSnap.vbat_v);
  bool esp_bat_valid   = validF(espSnap.vbat_v);
  bool ads_moist_valid = validF(adsSnap.moist_v);
  bool esp_moist_valid = validF(espSnap.moist_v);
  bool ads_res_valid   = validF(adsSnap.r_ohms);
  bool esp_res_valid   = validF(espSnap.r_ohms);
  bool ads_micro_valid = validF(adsSnap.micro_v);
  bool esp_micro_valid = validF(espSnap.micro_v);

  // Battery
  d.batteryVoltage = ads_bat_valid ? adsSnap.vbat_v :
                     (esp_bat_valid ? espSnap.vbat_v : NAN);
  d.batteryPercent = ads_bat_valid ? adsSnap.vbat_pct :
                     (esp_bat_valid ? espSnap.vbat_pct : NAN);

  // Moisture
  float moist_v_pref = ads_moist_valid ? adsSnap.moist_v :
                       (esp_moist_valid ? espSnap.moist_v : NAN);
  d.soilMoisture_mV     = validF(moist_v_pref) ? moist_v_pref * 1000.0f : NAN;
  d.soilMoisturePercent = ads_moist_valid ? adsSnap.moist_pct :
                          (esp_moist_valid ? espSnap.moist_pct : NAN);

  // Microbial
  d.microVoltage = ads_micro_valid ? adsSnap.micro_v :
                   (esp_micro_valid ? espSnap.micro_v : NAN);
  d.resistance   = ads_res_valid ? adsSnap.r_ohms :
                   (esp_res_valid ? espSnap.r_ohms : NAN);

  // ADS raw
  d.ads_v3v3       = ads_v3v3;
  d.ads_moist_v    = adsSnap.moist_v;
  d.ads_micro_v    = adsSnap.micro_v;
  d.ads_vbat_div_v = ads_vbat_div;

  // ESP raw
  d.esp_vbat_mV  = esp_vbat_mV;
  d.esp_moist_mV = esp_moist_mV;
  d.esp_micro_mV = esp_micro_mV;

  snprintf(d.nodeName, sizeof(d.nodeName), "%s", cfg.nickname.c_str());
}

// ======= PAYLOAD LOGGING (labels aligned with CSV) =========
void logPayload(const SensorData_t &d) {
  LOG("[Payload] timestamp_utc: %s\n", d.timestampUtc[0] ? d.timestampUtc : "(unset)");
  LOG("[Payload] node_id: %ld\n", (long)d.nodeId);
  LOG("[Payload] node_nickname: %s\n", d.nodeName);
  LOG("[Payload] lat_deg: %.6f\n", d.latitude);
  LOG("[Payload] lon_deg: %.6f\n", d.longitude);
  LOG("[Payload] co2_ppm: %.1f\n", d.co2PPM);
  LOG("[Payload] air_temp_C: %.2f\n", d.airTempC);
  LOG("[Payload] air_humidity_pct: %.2f\n", d.airHumidity);
  LOG("[Payload] soil_temp_C: %.2f\n", d.soilTempC);
  LOG("[Payload] battery_voltage_V: %.3f\n", d.batteryVoltage);
  LOG("[Payload] battery_percent_pct: %.1f\n", d.batteryPercent);
  LOG("[Payload] soil_moisture_mV: %.1f\n", d.soilMoisture_mV);
  LOG("[Payload] soil_moisture_pct: %.1f\n", d.soilMoisturePercent);
  LOG("[Payload] micro_voltage_V: %.4f\n", d.microVoltage);
  LOG("[Payload] resistance_ohms: %.2f\n", d.resistance);
  LOG("[Payload] ads_v3v3_V: %.3f\n", d.ads_v3v3);
  LOG("[Payload] ads_moist_V: %.3f\n", d.ads_moist_v);
  LOG("[Payload] ads_micro_V: %.3f\n", d.ads_micro_v);
  LOG("[Payload] ads_vbat_div_V: %.3f\n", d.ads_vbat_div_v);
  LOG("[Payload] esp_vbat_mV: %.1f\n", d.esp_vbat_mV);
  LOG("[Payload] esp_moist_mV: %.1f\n", d.esp_moist_mV);
  LOG("[Payload] esp_micro_mV: %.1f\n", d.esp_micro_mV);
}

// ================== ARCJSON =================
static String buildArcGisFeaturesJson(const SensorData_t &d) {
  const float kMaxNumeric = 99999.9f;

  auto validF_ = [](float v){ return isfinite(v); };
  auto clampF_ = [&](float v, float lo, float hi){
    if (!validF_(v)) return NAN;
    if (v < lo) v = lo; if (v > hi) v = hi; return v;
  };

  float resistance_ohms = d.resistance;
  bool  resistance_ok   = validF_(resistance_ohms) && resistance_ohms >= 0.0f;
  if (resistance_ok && resistance_ohms > kMaxNumeric) resistance_ohms = kMaxNumeric;

  float battery_v       = d.batteryVoltage;
  bool  battery_v_ok    = validF_(battery_v);

  float battery_pct_f   = clampF_(d.batteryPercent, 0.0f, 100.0f);
  bool  battery_ok      = validF_(battery_pct_f);
  int   battery_pct_i   = battery_ok ? (int)roundf(battery_pct_f) : 0;

  float soil_moist_pct  = clampF_(d.soilMoisturePercent, 0.0f, 100.0f);
  bool  soil_moist_ok   = validF_(soil_moist_pct);

  float soil_moist_mv   = d.soilMoisture_mV;
  bool  soil_moist_mv_ok= validF_(soil_moist_mv);

  float soil_temp_c     = clampF_(d.soilTempC, -50.0f, 125.0f);
  bool  soil_temp_ok    = validF_(soil_temp_c);

  float co2_ppm_f       = clampF_(d.co2PPM, 0.0f, 50000.0f);
  bool  co2_ok          = validF_(co2_ppm_f);
  int   co2_ppm_i       = co2_ok ? (int)roundf(co2_ppm_f) : 0;

  float air_temp_c      = clampF_(d.airTempC, -50.0f, 125.0f);
  bool  air_temp_ok     = validF_(air_temp_c);

  float air_hum_pct     = clampF_(d.airHumidity, 0.0f, 100.0f);
  bool  air_hum_ok      = validF_(air_hum_pct);

  float micro_v         = d.microVoltage;
  bool  micro_v_ok      = validF_(micro_v);

  float ads_v3v3        = d.ads_v3v3;
  float ads_moist_v     = d.ads_moist_v;
  float ads_micro_v     = d.ads_micro_v;
  float ads_vbat_div_v  = d.ads_vbat_div_v;

  float esp_vbat_mv     = d.esp_vbat_mV;
  float esp_moist_mv    = d.esp_moist_mV;
  float esp_micro_mv    = d.esp_micro_mV;

  float lat = clampF_(d.latitude,  -90.0f,  90.0f);
  float lon = clampF_(d.longitude, -180.0f, 180.0f);
  bool  geo_ok = validCoord(lat, lon);

  StaticJsonDocument<1500> doc;
  JsonArray  features = doc.to<JsonArray>();
  JsonObject feature  = features.createNestedObject();
  JsonObject attrs    = feature.createNestedObject("attributes");

  attrs["node_id"] = d.nodeId;

  // Timestamp
  if (d.timestampUtc[0] != '\0') {
    attrs["timestamp_utc"] = d.timestampUtc;
  } else {
    attrs["timestamp_utc"] = nullptr;
  }

  // Core metrics
  setOrNullF(attrs, "battery_voltage_v",      battery_v,        battery_v_ok);
  setOrNullI(attrs, "battery_percent",        battery_pct_i,    battery_ok);
  setOrNullF(attrs, "soil_moisture_percent",  soil_moist_pct,   soil_moist_ok);
  setOrNullF(attrs, "soil_moisture_mv",       soil_moist_mv,    soil_moist_mv_ok);
  setOrNullF(attrs, "soil_temp_c",            soil_temp_c,      soil_temp_ok);
  setOrNullF(attrs, "resistance_ohms",        resistance_ohms,  resistance_ok);

  setOrNullI(attrs, "co2_ppm",                co2_ppm_i,        co2_ok);
  setOrNullF(attrs, "air_temp_c",             air_temp_c,       air_temp_ok);
  setOrNullF(attrs, "air_humidity_percent",   air_hum_pct,      air_hum_ok);

  // Microbial node voltage
  setOrNullF(attrs, "micro_voltage_v",        micro_v,          micro_v_ok);

  // ADS raw
  setOrNullF(attrs, "ads_v3v3_v",             ads_v3v3,         validF_(ads_v3v3));
  setOrNullF(attrs, "ads_moist_v",            ads_moist_v,      validF_(ads_moist_v));
  setOrNullF(attrs, "ads_micro_v",            ads_micro_v,      validF_(ads_micro_v));
  setOrNullF(attrs, "ads_vbat_div_v",         ads_vbat_div_v,   validF_(ads_vbat_div_v));

  // ESP raw (mV)
  setOrNullF(attrs, "esp_vbat_mv",            esp_vbat_mv,      validF_(esp_vbat_mv));
  setOrNullF(attrs, "esp_moist_mv",           esp_moist_mv,     validF_(esp_moist_mv));
  setOrNullF(attrs, "esp_micro_mv",           esp_micro_mv,     validF_(esp_micro_mv));

  attrs["node_nickname"] = d.nodeName;

  if (geo_ok) {
    JsonObject geom = feature.createNestedObject("geometry");
    geom["x"] = lon;
    geom["y"] = lat;
    JsonObject sr = geom.createNestedObject("spatialReference");
    sr["wkid"] = 4326;
  }

  String json; serializeJson(doc, json);
  return json;
}

// ================== WIFI / TIME / CLOUD =================
bool connectWiFiSTA(const String& ssid, const String& pass, uint32_t timeoutMs=20000) {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid.c_str(), pass.c_str());
  uint32_t t0 = millis();
  while (WiFi.status()!=WL_CONNECTED && millis()-t0<timeoutMs) delay(200);
  return (WiFi.status()==WL_CONNECTED);
}

bool fetchTimeUTC(char *out, size_t len) {
  // UTC
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  struct tm timeinfo;
  const uint32_t timeoutMs = 10000;
  uint32_t start = millis();

  while (millis() - start < timeoutMs) {
    if (getLocalTime(&timeinfo, 1000)) {
      strftime(out, len, "%Y-%m-%dT%H:%M:%SZ", &timeinfo);
      LOG("[Time] %s\n", out);
      return true;
    }
  }
  LOG("[ERROR] [Time] Failed to get NTP time\n");
  if (len > 0) out[0] = '\0';
  return false;
}

bool UploadToCloudOnce(const SensorData_t &d, String &errOut) {
  errOut = "";

  if (!cfg.wifi_ssid.length()) {
    errOut = "No Wi-Fi configured";
    return false;
  }
  if (WiFi.status() != WL_CONNECTED) {
    errOut = "Wi-Fi not connected";
    return false;
  }

  String featuresJson = buildArcGisFeaturesJson(d);
  String body = "f=json&rollbackOnFailure=false&features=" + urlEncode(featuresJson);
  body += "&token=" + urlEncode(ARC_API_KEY);
  String fullUrl = String(ARC_ENDPOINT) + "?token=" + urlEncode(ARC_API_KEY);

  WiFiClientSecure client; client.setInsecure();
  HTTPClient http; http.setTimeout(20000);

  bool okHttp=false, okApi=false;
  String resp;

  if (http.begin(client, fullUrl)) {
    http.addHeader("Content-Type", "application/x-www-form-urlencoded");
    http.addHeader("Referer", ARC_REFERER);
    int code = http.POST(body);
    okHttp = (code >= 200 && code < 300);
    if (code > 0) resp = http.getString();
    else errOut = String("HTTP error code ") + code;
    http.end();
  } else {
    errOut = "HTTP begin() failed";
  }

  if (okHttp && resp.length()) {
    StaticJsonDocument<1024> doc;
    DeserializationError derr = deserializeJson(doc, resp);
    if (!derr) {
      JsonArray results = doc["addResults"];
      if (!results.isNull() && results.size() > 0) {
        okApi = results[0]["success"] | false;
        if (!okApi) {
          const char* desc = results[0]["error"]["description"] | "";
          int code = results[0]["error"]["code"] | 0;
          errOut = String("ArcGIS addFeatures failed (") + code + "): " + desc;
        }
      } else {
        errOut = "ArcGIS addResults missing/empty";
      }
    } else {
      errOut = String("Cloud JSON parse error: ") + derr.c_str();
    }
  }

  WiFi.disconnect(true, true);
  return okHttp && okApi;
}

// ================== SD CARD (CSV LOGGING) =================
void initSD() {
  pinMode(SD_CD, INPUT_PULLUP);
  int cd = digitalRead(SD_CD);
  if (cd == HIGH) {
    LOG("[ERROR] [SD] No card detected (CD HIGH)\n");
    g_sd_ok = false;
    return;
  }

  SPI.begin(SD_SCK, SD_MISO, SD_MOSI, SD_CS);

  if (!SD.begin(SD_CS)) {
    LOG("[ERROR] [SD] SD.begin failed\n");
    g_sd_ok = false;
    return;
  }
  g_sd_ok = true;
}

bool sdAppendSample(const SensorData_t &d) {
  if (!g_sd_ok) {
    LOG("[ERROR] [SD] Not initialized, skipping log\n");
    return false;
  }

  bool writeHeader = false;

  if (!SD.exists("/DirtData.csv")) {
    writeHeader = true;
  }

  File f = SD.open("/DirtData.csv", FILE_APPEND);
  if (!f) {
    LOG("[ERROR] [SD] Failed to open DirtData.csv\n");
    return false;
  }

  if (f.size() == 0) {
    writeHeader = true;
  }

  if (writeHeader) {
    f.println(
      "timestamp_utc,"
      "node_id,"
      "node_nickname,"
      "lat_deg,"
      "lon_deg,"
      "co2_ppm,"
      "air_temp_C,"
      "air_humidity_pct,"
      "soil_temp_C,"
      "battery_voltage_V,"
      "battery_percent_pct,"
      "soil_moisture_mV,"
      "soil_moisture_pct,"
      "micro_voltage_V,"
      "resistance_ohms,"
      "ads_v3v3_V,"
      "ads_moist_V,"
      "ads_micro_V,"
      "ads_vbat_div_V,"
      "esp_vbat_mV,"
      "esp_moist_mV,"
      "esp_micro_mV"
    );
  }

  if (d.timestampUtc[0] != '\0') f.print(d.timestampUtc);
  f.print(',');

  f.print(d.nodeId);               f.print(',');
  f.print(d.nodeName);             f.print(',');
  f.print(d.latitude, 6);          f.print(',');
  f.print(d.longitude, 6);         f.print(',');
  f.print(d.co2PPM, 1);            f.print(',');
  f.print(d.airTempC, 2);          f.print(',');
  f.print(d.airHumidity, 2);       f.print(',');
  f.print(d.soilTempC, 2);         f.print(',');
  f.print(d.batteryVoltage, 3);    f.print(',');
  f.print(d.batteryPercent, 1);    f.print(',');
  f.print(d.soilMoisture_mV, 1);   f.print(',');
  f.print(d.soilMoisturePercent,1);f.print(',');
  f.print(d.microVoltage, 4);      f.print(',');
  f.print(d.resistance, 2);        f.print(',');
  f.print(d.ads_v3v3, 3);          f.print(',');
  f.print(d.ads_moist_v, 3);       f.print(',');
  f.print(d.ads_micro_v, 3);       f.print(',');
  f.print(d.ads_vbat_div_v, 3);    f.print(',');
  f.print(d.esp_vbat_mV, 1);       f.print(',');
  f.print(d.esp_moist_mV, 1);      f.print(',');
  f.print(d.esp_micro_mV, 1);

  f.println();
  f.close();

  LOG("[SD] Data posted\n");
  return true;
}

// ================== SLEEP =================
void SleepMinutes(uint32_t minutes) {
  uint64_t us = (uint64_t)minutes * 60ULL * 1000000ULL;
  LOG("[Run] Complete, sleeping %lu min\n", (unsigned long)minutes);
  esp_sleep_enable_timer_wakeup(us);
  esp_deep_sleep_start();
}

// ================== MODE DECISION =================
bool bootButtonHeld() {
  pinMode(BOOT_BTN, INPUT_PULLUP);
  delay(10);
  return digitalRead(BOOT_BTN)==LOW;
}
bool needsWizard() {
  if (bootButtonHeld()) return true;
  if (!cfg.lat.length() || !cfg.lon.length()) return true;
  if (!cfg.wifi_ssid.length()) return true;
  return false;
}

// ================== ARDUINO ======================
void setup() {
#if DEBUG
  Serial.begin(115200);
  delay(1000);  // 1s after boot for USB to settle
#endif

  esp_log_level_set("i2c", ESP_LOG_NONE);

  LOG("\n[Setup] Booting\n");
  LOG("[Setup] Mode: one-shot sample + deep sleep (DEBUG=%d)\n", DEBUG);

  // Power rails
  pinMode(ESP_PWR_3V3, OUTPUT); digitalWrite(ESP_PWR_3V3, LOW);
  pinMode(ESP_GPS_3V3, OUTPUT); digitalWrite(ESP_GPS_3V3, LOW);

  // ADC config
  analogReadResolution(12);
  analogSetAttenuation(ADC_11db);
  pinMode(ESP_ADC_SEN,   INPUT);
  pinMode(ESP_ADC_VBAT,  INPUT);
  pinMode(ESP_ADC_MOIST, INPUT);
  pinMode(ESP_TEMP,      INPUT);

  // Load config
  loadPrefs();

  // Wizard mode
  if (needsWizard()) {
    LOG("[Setup] Complete (wizard mode) — starting captive portal\n");
    startAP(); startDNS(); startHTTP();
    while (true) { dns.processNextRequest(); server.handleClient(); delay(5); }
  }

  LOG("[Setup] Complete — starting measurement\n");

  // Run mode: single shot
  digitalWrite(ESP_PWR_3V3, HIGH);
  LOG("[PWR] Sensor rail ON\n");
  delay(20);

  // Init SCD4x
  g_scd_present = scdInitRobust();

  // Init ADS1115 on same bus
  g_ads_present = adsInit();
  lowPowerWaitMs(100);   // give ADS time before first read

  // Wait for first SCD sample (using data-ready + light sleep/delay)
  scdWaitAndRead(6500);

  // Take ONE ADS + ESP snapshot + soil temp
  float ads_v3v3    = NAN;
  float ads_vbat_div= NAN;
  float esp_vbat_mV = NAN;
  float esp_moist_mV= NAN;
  float esp_micro_mV= NAN;

  AnalogSnapshot_t adsSnap = readAdsSnapshot(ads_v3v3, ads_vbat_div);
  AnalogSnapshot_t espSnap = readEspSnapshot(esp_vbat_mV, esp_moist_mV, esp_micro_mV);
  float soilTempC          = getSoilTempC();

  // Build SensorData_t from snapshots
  SensorData_t sample{};
  buildSensorDataFromSnapshots(sample,
                               adsSnap,
                               espSnap,
                               ads_v3v3,
                               ads_vbat_div,
                               esp_vbat_mV,
                               esp_moist_mV,
                               esp_micro_mV,
                               soilTempC);

  // Init SD once (if card present)
  initSD();

  // Wi-Fi / Time
  bool wifi_ok = false;
  if (!cfg.wifi_ssid.length()) {
    LOG("[ERROR] [Cloud] No Wi-Fi configured\n");
  } else {
    wifi_ok = connectWiFiSTA(cfg.wifi_ssid, cfg.wifi_pass);
    if (!wifi_ok) {
      LOG("[ERROR] [Cloud] Wi-Fi connect failed\n");
    } else {
      fetchTimeUTC(sample.timestampUtc, sizeof(sample.timestampUtc));
    }
  }

  // Payload logging (after timestamp attempt)
  logPayload(sample);

  // SD log (with whatever timestamp we have)
  sdAppendSample(sample);

  // Cloud upload
  if (wifi_ok) {
    String cloudErr;
    bool ok = UploadToCloudOnce(sample, cloudErr);
    if (ok) {
      LOG("[Cloud] Data posted\n");
    } else if (cloudErr.length()) {
      LOG("[ERROR] [Cloud] %s\n", cloudErr.c_str());
    } else {
      LOG("[ERROR] [Cloud] Unknown upload failure\n");
    }
  }

  // Cleanly stop SCD before cutting power
  scdStop();
  delay(5);

  // Power down sensor rail
  digitalWrite(ESP_PWR_3V3, LOW);
  LOG("[PWR] Sensor rail OFF\n");

  // Deep sleep until next interval
  uint32_t minutes = cfg.interval_min ? cfg.interval_min : 30;
  SleepMinutes(minutes);
}

void loop() {
  // never reached (deep sleep from setup)
}
