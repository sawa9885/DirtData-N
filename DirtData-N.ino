/************************************************************
 * DirtData Node (ESP32-C6, Arduino core)
 * - Captive-portal Setup Wizard (SoftAP + WebServer + DNSServer + NVS)
 * - Two modes:
 *     1) "direct_wifi"  -> join Wi-Fi and POST once to ArcGIS, then deep sleep
 *     2) "device_network" (hub) -> ESP-NOW send once to hub, then deep sleep
 * - Interval (minutes) controls DEEP SLEEP between wakeups
 * - SCD4x bring-up hardened: power -> I2C -> probe -> stop -> reinit -> start
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
#include <esp_now.h>
#include <esp_wifi.h>

// ---- Sensors ----
#include <OneWireNg_CurrentPlatform.h>
#include <drivers/DSTherm.h>
#include <utils/Placeholder.h>
#include <SensirionI2cScd4x.h>

// ================== DEBUG ==================
#define DEBUG 1
#if DEBUG
  #define LOG(...) do { Serial.printf(__VA_ARGS__); } while(0)
#else
  #define LOG(...) do {} while(0)
#endif

// ================== PINS ===================
#define ESP_ADC_VCC   2
#define ESP_ADC_GND   3
#define ESP_ADC_SEN   4
#define ESP_ADC_VBAT  5
#define ESP_ADC_MOIST 6
#define ESP_PWR_3V3   0    // SENSOR rail enable (keep LOW until run-mode)
#define ESP_GPS_3V3   1
#define ESP_SDA      10
#define ESP_SCL      11
#define ESP_TEMP      7
#define BOOT_BTN      9

// ============== CONSTANTS ==================
const float R_KNOWN             = 1000.0f;
const float DIVIDER_RATIO       = 2.0f;
const float BATTERY_VOLTAGE_MAX = 4.5f;
const float BATTERY_VOLTAGE_MIN = 2.0f;
const float MOISTURE_MV_DRY     = 3300.0f;
const float MOISTURE_MV_WET     = 1000.0f;

// I2C
#define I2C_SPEED_HZ 100000UL
const uint8_t SCD4X_ADDR = 0x62;

// SCD4x state
SensirionI2cScd4x scd4x;
static bool g_scd_present = false;
static bool g_scd_running = false;
static uint8_t g_scd_fail_streak = 0;
static float scd_co2_ppm=-1, scd_temp_c=-1, scd_rh=-1;

// ============== HUB (ESP-NOW) ==============
#define ESPNOW_CHANNEL 6
uint8_t hubAddress[] = {0x10, 0x51, 0xDB, 0x00, 0x26, 0x60}; // hub STA MAC

// ============== CLOUD (ArcGIS) =============
const char *ARC_ENDPOINT = "https://gis.dirtdata.ai/arcgis/rest/services/dirtdata/Dirt_Data_Nodes_POC/FeatureServer/0/addFeatures";
const char *ARC_REFERER  = "https://sensor.dirtdata.ai";
const char *ARC_API_KEY  = "AAPTg-FeepoS1ejSr4QBtPc02Zp-UeIfxKMnp96qOukL9MTt1OknOyChtkEQysGxYXi61t4XXUVHo7KQJEdrOxPhe3DSuFOgb46JJ8fUniAd0oPlBpC8hjn-V0kCDuUKtfqP0bon1bLJM5briX_nXmRgiogHzqhPOFLgM8k7hr7VxWRlq_C7GL7XfMmJt959xRSJ1XovVDrjkCd5J30Qhc_vFAayyoZyPmhCDSPTSkvQJnY.AT1_RjRfFhTZ";

// ================== CONFIG =================
Preferences prefs;

struct Config {
  String nickname;          // "node_nickname"
  String lat;               // text, saved with 6 decimals
  String lon;
  uint32_t interval_min;    // deep sleep minutes
  String conn_method;       // "direct_wifi" | "device_network"
  String wifi_ssid;         // direct_wifi only
  String wifi_pass;         // direct_wifi only
} cfg;

// ================== PAYLOAD =================
struct SensorData_t {
  float resistance;            // ohms
  float batteryPercent;        // %
  float soilMoisturePercent;   // %
  float soilTempC;             // °C (DS18B20)
  float co2PPM;                // ppm (SCD41)
  float airTempC;              // °C (SCD41)
  float airHumidity;           // %RH (SCD41)
  float latitude;
  float longitude;
  char  nodeName[32];
};

// Use MAC-derived 31-bit positive integer as node_id (ArcGIS integer)
int32_t GetNodeIdFromMac() {
  uint64_t mac = ESP.getEfuseMac();
  uint32_t h = 2166136261u;         // FNV-1a 32-bit
  for (int i = 0; i < 6; ++i) {
    uint8_t b = (mac >> (8 * i)) & 0xFF;
    h ^= b; h *= 16777619u;
  }
  return (int32_t)(h & 0x7FFFFFFF);
}

// ================== SETUP WIZARD ===========
WebServer server(80);
DNSServer dns;
const byte DNS_PORT = 53;

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

// ---------- HTML helpers ----------
String iosHead(const char* title) {
  String css; css.reserve(2600);
  css += R"HTML(
<!doctype html><html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="viewport-fit=cover,width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>)HTML"; css += title; css += R"HTML(</title>
<style>
:root{--blue:#0A84FF;--text:#0A0A0A;--muted:#6E6E73;--bg:#fff;--card:#fff;--shadow:0 6px 24px rgba(0,0,0,.08);--radius:16px}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{margin:0;padding:0;background:var(--bg);color:var(--text);font:16px -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Inter,Helvetica,Arial,sans-serif}
.safe{padding:env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left)}
.wrap{min-height:100dvh;display:flex;align-items:center;justify-content:center;padding:24px}
.card{max-width:720px;width:100%;background:var(--card);border-radius:var(--radius);box-shadow:var(--shadow);padding:28px}
.title{font-size:28px;font-weight:700;letter-spacing:-.02em;margin:4px 0 8px}
.subtitle{font-size:15px;color:var(--muted);margin:0 0 18px}
.brand{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.chip{display:inline-block;padding:6px 10px;border-radius:999px;background:#E6F0FF;color:#003EAA;font-weight:600;font-size:12px;letter-spacing:.02em}
.hero{margin:16px 0 20px;line-height:1.6}
.row{display:flex;flex-direction:column;gap:10px;margin:12px 0}
.label{font-size:13px;color:#6E6E73;margin-bottom:2px}
.input,.button{width:100%;border-radius:14px;padding:14px 16px;font-size:16px;border:1px solid #E5E5EA;background:#fff;outline:none}
.input:focus{border-color:#0A84FF;box-shadow:0 0 0 3px rgba(10,132,255,.15)}
.actions{display:flex;gap:12px;flex-wrap:wrap;margin-top:18px}
.btn{appearance:none;border:0;border-radius:14px;padding:14px 18px;font-weight:700;font-size:16px;letter-spacing:.2px;text-decoration:none;display:inline-flex;align-items:center;gap:8px;cursor:pointer}
.btn-primary{background:var(--blue);color:#fff;box-shadow:0 4px 12px rgba(10,132,255,.35)}
.btn-secondary{background:#EEF2FF;color:#103D98}
.btn-link{background:transparent;color:#0A84FF;padding:10px 0}
.small{font-size:13px;color:#6E6E73}
.meta{margin-top:14px;font-size:12px;color:var(--muted)}
.footer{margin-top:14px;font-size:12px;color:var(--muted)}
.radio-group{display:grid;gap:12px}
.radio{display:flex;gap:12px;align-items:flex-start;border:1px solid #E5E5EA;border-radius:14px;padding:12px 14px}
.radio input{margin-top:3px}
.badge{display:inline-block;background:#FFF7E6;color:#8A5200;border:1px solid #FFE1A6;border-radius:999px;padding:4px 8px;font-size:12px}
.err{color:#B00020;font-size:13px;margin-top:6px}
.ok{color:#007A0A;font-size:13px;margin-top:6px}
</style>
</head><body><div class="safe wrap"><main class="card">
<div class="brand">
  <svg width="28" height="28" viewBox="0 0 24 24" fill="none" aria-hidden="true">
    <rect x="3" y="3" width="18" height="18" rx="6" fill="#0A84FF"/>
    <path d="M8.2 12.4c2.1-2.6 5.5-2.6 7.6 0" stroke="#fff" stroke-width="2" stroke-linecap="round"/>
    <circle cx="8.8" cy="13.8" r="1.2" fill="#fff"/>
    <circle cx="15.2" cy="13.8" r="1.2" fill="#fff"/>
  </svg>
  <span class="chip">DirtData</span>
</div>
)HTML";
  return css;
}
String iosFoot(const String& meta) {
  String f; f.reserve(600);
  f += R"HTML(<div class="meta">)HTML"; f += meta; f += R"HTML(</div>
<div class="footer">© )HTML"; f += String(__DATE__); f += R"HTML( DirtData • All rights reserved</div>
</main></div></body></html>)HTML";
  return f;
}

// ---------- Wizard Pages ----------
String pageWelcome() {
  String html; html.reserve(6000);
  html += iosHead("DirtData • Welcome");
  html += R"HTML(
<h1 class="title">Welcome — thanks for choosing DirtData!</h1>
<p class="subtitle">Setup • Page 1 of 6</p>
<p class="hero">This wizard configures your node name, location, upload interval, and connection method.</p>
<div class="actions">
  <a class="btn btn-primary" href="/device-name">Get Started</a>
</div>
)HTML";
  String meta = "SSID: <b>DirtData-Setup-" + macLast4() + "</b> • Device ID: <b>" + macStr() + "</b>";
  html += iosFoot(meta);
  return html;
}

String pageDeviceName() {
  String html; html.reserve(7000);
  html += iosHead("DirtData • Device Name & ID");
  html += R"HTML(
<a class="btn btn-link" href="/">← Back</a>
<h1 class="title">Name your device</h1>
<p class="subtitle">Setup • Page 2 of 6</p>
<form method="POST" action="/device-name" autocomplete="on">
  <fieldset class="row">
    <label class="label" for="nickname">Device Nickname</label>
    <input class="input" id="nickname" name="nickname" type="text" minlength="1" maxlength="40"
           placeholder="e.g., North Plot Node" value=")HTML";
  html += cfg.nickname; html += R"HTML(" required>
    <span class="small">You can change this later.</span>
  </fieldset>
  <div class="actions">
    <button class="btn btn-secondary" type="button" onclick="history.back()">Back</button>
    <button class="btn btn-primary" type="submit">Continue</button>
  </div>
</form>
)HTML";
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

static String fmtCoord(double v, int decimals = 6) {
  char buf[32]; dtostrf(v, 0, decimals, buf); return String(buf);
}

String pageDeviceLocation() {
  String html; html.reserve(11000);
  html += iosHead("DirtData • Device Location");
  html += R"HTML(
<a class="btn btn-link" href="/device-name">← Back</a>
<h1 class="title">Device location</h1>
<p class="subtitle">Setup • Page 3 of 6</p>

<div id="gpsWarn" class="small" style="display:none"><span class="badge">Note</span> Your browser may block GPS on captive/HTTP pages. If the button does nothing, enter coordinates manually.</div>

<div class="actions">
  <button class="btn btn-secondary" type="button" id="useGPS">Use phone GPS</button>
</div>

<form id="locform" method="POST" action="/device-location" autocomplete="on" novalidate>
  <fieldset class="row">
    <label class="label" for="lat">Latitude (−90..90)</label>
    <input class="input" id="lat" name="lat" type="text" inputmode="text"
           placeholder="40.014990" value=")HTML"; html += cfg.lat; html += R"HTML(" required>
    <div id="latErr" class="err" style="display:none">Enter a latitude between −90 and 90.</div>
  </fieldset>
  <fieldset class="row">
    <label class="label" for="lon">Longitude (−180..180)</label>
    <input class="input" id="lon" name="lon" type="text" inputmode="text"
           placeholder="-105.270550" value=")HTML"; html += cfg.lon; html += R"HTML(" required>
    <div id="lonErr" class="err" style="display:none">Enter a longitude between −180 and 180.</div>
  </fieldset>
  <div class="actions">
    <button class="btn btn-secondary" type="button" onclick="history.back()">Back</button>
    <button class="btn btn-primary" type="submit">Continue</button>
  </div>
</form>

<script>
(function(){ if(!window.isSecureContext){ document.getElementById('gpsWarn').style.display='block'; } })();
(function(){
  const btn = document.getElementById('useGPS');
  const lat = document.getElementById('lat');
  const lon = document.getElementById('lon');
  if(!btn) return;
  btn.addEventListener('click', function(){
    if(!('geolocation' in navigator)){ alert('Geolocation not supported.'); return; }
    const old = btn.textContent; btn.disabled = true; btn.textContent='Getting location…';
    navigator.geolocation.getCurrentPosition(function(pos){
      lat.value = pos.coords.latitude.toFixed(6);
      lon.value = pos.coords.longitude.toFixed(6);
      btn.textContent=old; btn.disabled=false;
    }, function(err){
      alert('Could not get GPS; please enter manually.');
      btn.textContent=old; btn.disabled=false;
    }, {enableHighAccuracy:true, timeout:15000, maximumAge:0});
  });
})();
(function(){
  const form=document.getElementById('locform'), lat=document.getElementById('lat'), lon=document.getElementById('lon');
  const latErr=document.getElementById('latErr'), lonErr=document.getElementById('lonErr');
  function vLat(v){const x=parseFloat(v);return isFinite(x)&&x>=-90&&x<=90;}
  function vLon(v){const x=parseFloat(v);return isFinite(x)&&x>=-180&&x<=180;}
  form.addEventListener('submit', function(e){
    let ok=true; if(!vLat(lat.value)){latErr.style.display='block';ok=false;} else latErr.style.display='none';
    if(!vLon(lon.value)){lonErr.style.display='block';ok=false;} else lonErr.style.display='none';
    if(!ok)e.preventDefault();
  });
})();
</script>
)HTML";
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageAdvancedOptions() {
  String html; html.reserve(8000);
  html += iosHead("DirtData • Advanced Options");
  html += R"HTML(
<a class="btn btn-link" href="/device-location">← Back</a>
<h1 class="title">Advanced options</h1>
<p class="subtitle">Setup • Page 4 of 6</p>
<form method="POST" action="/advanced-options" autocomplete="on">
  <fieldset class="row">
    <label class="label" for="interval">Interval (minutes)</label>
    <input class="input" id="interval" name="interval" type="number" inputmode="numeric" step="1" min="1" max="10080"
           placeholder="30" value=")HTML"; html += String(cfg.interval_min); html += R"HTML(" required>
    <span class="small">More frequent readings reduce battery life.</span>
  </fieldset>
  <div class="actions">
    <button class="btn btn-secondary" type="button" onclick="history.back()">Back</button>
    <button class="btn btn-primary" type="submit">Continue</button>
  </div>
</form>
)HTML";
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageConnectionMethod() {
  String html; html.reserve(9000);
  html += iosHead("DirtData • Connection Method");
  html += R"HTML(
<a class="btn btn-link" href="/advanced-options">← Back</a>
<h1 class="title">Connection method</h1>
<p class="subtitle">Setup • Page 5 of 6</p>
<form method="POST" action="/connection-method" autocomplete="on">
  <div class="radio-group">
    <label class="radio">
      <input type="radio" name="conn" value="direct_wifi")HTML";
  if (cfg.conn_method=="direct_wifi") html += " checked";
  html += R"HTML(>
      <div><b>Direct Wi-Fi</b><div class="small">Node joins your Wi-Fi and uploads directly to the cloud.</div></div>
    </label>
    <label class="radio">
      <input type="radio" name="conn" value="device_network")HTML";
  if (cfg.conn_method=="device_network") html += " checked";
  html += R"HTML(>
      <div><b>Device Network (Hub)</b><div class="small">Node sends via ESP-NOW to a nearby DirtData hub.</div></div>
    </label>
  </div>
  <div class="actions" style="margin-top:18px">
    <button class="btn btn-secondary" type="button" onclick="history.back()">Back</button>
    <button class="btn btn-primary" type="submit">Continue</button>
  </div>
</form>
)HTML";

  html += R"HTML(<div class="hero" style="margin-top:18px"><b>Current selections:</b><br>)HTML";
  html += "Nickname: <b>"; html += (cfg.nickname.length()?cfg.nickname:String("(none)")); html += "</b><br>";
  html += "Location: <b>"; html += (cfg.lat.length()?cfg.lat:String("(lat?)")); html += ", "; html += (cfg.lon.length()?cfg.lon:String("(lon?)")); html += "</b><br>";
  html += "Interval: <b>"; html += String(cfg.interval_min); html += " min</b>";
  html += R"HTML(</div>)HTML";
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageWifiCreds(const String& statusMsg = "", bool ok=false) {
  String html; html.reserve(9000);
  html += iosHead("DirtData • Wi-Fi");
  html += R"HTML(
<a class="btn btn-link" href="/connection-method">← Back</a>
<h1 class="title">Wi-Fi credentials</h1>
<p class="subtitle">Setup • Page 6 of 6</p>
<form method="POST" action="/wifi" autocomplete="on">
  <fieldset class="row">
    <label class="label" for="ssid">SSID</label>
    <input class="input" id="ssid" name="ssid" type="text" minlength="1" maxlength="64"
           placeholder="Your Wi-Fi name" value=")HTML"; html += cfg.wifi_ssid; html += R"HTML(" required>
  </fieldset>
  <fieldset class="row">
    <label class="label" for="pass">Password</label>
    <input class="input" id="pass" name="pass" type="password" maxlength="64"
           placeholder="Your Wi-Fi password" value=")HTML"; html += cfg.wifi_pass; html += R"HTML(">
  </fieldset>
  <div class="actions">
    <button class="btn btn-secondary" type="button" onclick="history.back()">Back</button>
    <button class="btn btn-primary" type="submit">Test & Save</button>
  </div>
</form>
)HTML";
  if (statusMsg.length()) {
    html += String("<div class=\"") + (ok?"ok":"err") + "\">" + statusMsg + "</div>";
  }
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

String pageSaved() {
  String html; html.reserve(5000);
  html += iosHead("DirtData • Setup Saved");
  html += R"HTML(
<h1 class="title">Setup saved</h1>
<p class="subtitle">You're good to go. You may close this page.</p>
<div class="hero">
)HTML";
  html += "Nickname: <b>"; html += (cfg.nickname.length()?cfg.nickname:String("(none)")); html += "</b><br>";
  html += "Location: <b>"; html += (cfg.lat.length()?cfg.lat:String("(lat?)")); html += ", "; html += (cfg.lon.length()?cfg.lon:String("(lon?)")); html += "</b><br>";
  html += "Interval: <b>"; html += String(cfg.interval_min); html += " min</b><br>";
  html += "Connection: <b>"; html += (cfg.conn_method=="direct_wifi"?"Direct Wi-Fi":"Device Network (Hub)"); html += "</b>";
  html += R"HTML(</div>
<div class="actions"><a class="btn btn-primary" href="/">Finish</a></div>
)HTML";
  html += iosFoot("Device ID: <b>" + macStr() + "</b>");
  return html;
}

// ---------- HTTP handlers ----------
void handleRoot() { server.send(200, "text/html; charset=utf-8", pageWelcome()); }

void handleGetDeviceName() { server.send(200, "text/html; charset=utf-8", pageDeviceName()); }
void handlePostDeviceName() {
  if (server.hasArg("nickname")) {
    String v = server.arg("nickname"); if (v.length() > 40) v.remove(40);
    cfg.nickname = v; prefs.putString("nickname", cfg.nickname);
  }
  server.sendHeader("Location", "/device-location", true); server.send(303, "text/plain", "");
}

void handleGetDeviceLocation() { server.send(200, "text/html; charset=utf-8", pageDeviceLocation()); }
static String clampCoordStr(const String& s, float lo, float hi) {
  double x = atof(s.c_str());
  if (isnan(x) || isinf(x)) return String("");
  if (x < lo) x = lo; if (x > hi) x = hi;
  return fmtCoord(x, 6);
}
void handlePostDeviceLocation() {
  if (server.hasArg("lat")) { cfg.lat = clampCoordStr(server.arg("lat"), -90, 90); prefs.putString("lat", cfg.lat); }
  if (server.hasArg("lon")) { cfg.lon = clampCoordStr(server.arg("lon"), -180, 180); prefs.putString("lon", cfg.lon); }
  server.sendHeader("Location", "/advanced-options", true); server.send(303, "text/plain", "");
}

void handleGetAdvancedOptions() { server.send(200, "text/html; charset=utf-8", pageAdvancedOptions()); }
void handlePostAdvancedOptions() {
  if (server.hasArg("interval")) {
    long v = server.arg("interval").toInt();
    if (v < 1) v = 1; if (v > 10080) v = 10080;
    cfg.interval_min = (uint32_t)v; prefs.putUInt("interval_min", cfg.interval_min);
  }
  server.sendHeader("Location", "/connection-method", true); server.send(303, "text/plain", "");
}

void handleGetConnectionMethod() { server.send(200, "text/html; charset=utf-8", pageConnectionMethod()); }
void handlePostConnectionMethod() {
  if (server.hasArg("conn")) {
    String v = server.arg("conn");
    if (v=="direct_wifi" || v=="device_network") { cfg.conn_method = v; prefs.putString("conn_method", cfg.conn_method); }
  }
  if (cfg.conn_method=="direct_wifi") {
    server.sendHeader("Location", "/wifi", true); server.send(303, "text/plain", "");
  } else {
    server.send(200, "text/html; charset=utf-8", pageSaved());
  }
}

void handleGetWifi() { server.send(200, "text/html; charset=utf-8", pageWifiCreds()); }
bool testStaJoin(const String& ssid, const String& pass, uint32_t timeoutMs=15000) {
  WiFi.mode(WIFI_AP_STA);
  WiFi.begin(ssid.c_str(), pass.c_str());
  uint32_t t0=millis();
  while (WiFi.status()!=WL_CONNECTED && millis()-t0<timeoutMs) delay(200);
  bool ok = (WiFi.status()==WL_CONNECTED);
  if (ok) LOG("[WiFi Test] Connected to %s, IP=%s\n", ssid.c_str(), WiFi.localIP().toString().c_str());
  else     LOG("[WiFi Test] Failed to connect to %s\n", ssid.c_str());
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
    cfg.wifi_ssid = ssid; cfg.wifi_pass = pass;
    prefs.putString("wifi_ssid", cfg.wifi_ssid);
    prefs.putString("wifi_pass", cfg.wifi_pass);
    server.send(200, "text/html; charset=utf-8", pageSaved());
  } else {
    server.send(200, "text/html; charset=utf-8", pageWifiCreds("Could not join that Wi-Fi. Check SSID/password and try again.", false));
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
  bool ok = WiFi.softAP(ssid.c_str(), nullptr, 6, 0, 4);
  IPAddress ip = WiFi.softAPIP();
  Serial.printf("[AP] SSID: %s | IP: %s | %s\n", ssid.c_str(), ip.toString().c_str(), ok?"OK":"FAILED");
  Serial.println("[Hint] If captive pop-up doesn't appear, visit: http://192.168.4.1/");
}
void startDNS() {
  IPAddress apIP = WiFi.softAPIP();
  dns.start(DNS_PORT, "*", apIP);
  Serial.printf("[DNS] Captive DNS started on %s\n", apIP.toString().c_str());
}
void startHTTP() {
  server.on("/", HTTP_GET, handleRoot);

  server.on("/device-name", HTTP_GET, handleGetDeviceName);
  server.on("/device-name", HTTP_POST, handlePostDeviceName);

  server.on("/device-location", HTTP_GET, handleGetDeviceLocation);
  server.on("/device-location", HTTP_POST, handlePostDeviceLocation);

  server.on("/advanced-options", HTTP_GET, handleGetAdvancedOptions);
  server.on("/advanced-options", HTTP_POST, handlePostAdvancedOptions);

  server.on("/connection-method", HTTP_GET, handleGetConnectionMethod);
  server.on("/connection-method", HTTP_POST, handlePostConnectionMethod);

  server.on("/wifi", HTTP_GET, handleGetWifi);
  server.on("/wifi", HTTP_POST, handlePostWifi);

  // Captive probes
  server.on("/hotspot-detect.html", HTTP_GET, handleAppleCaptive);
  server.on("/generate_204", HTTP_GET, handleAndroidCaptive);
  server.on("/gen_204", HTTP_GET, handleAndroidCaptive);
  server.on("/ncsi.txt", HTTP_GET, handleWindowsCaptive);
  server.on("/connecttest.txt", HTTP_GET, handleWindowsCaptive);
  server.on("/fwlink", HTTP_GET, handleWindowsCaptive);

  server.onNotFound(handleRedirectToRoot);
  server.begin();
  Serial.println("[HTTP] Server started on :80");
}
void loadPrefs() {
  prefs.begin("dirtdata", false);
  cfg.nickname     = prefs.getString("nickname", "");
  cfg.lat          = prefs.getString("lat", "");
  cfg.lon          = prefs.getString("lon", "");
  cfg.interval_min = prefs.getUInt("interval_min", 30);
  cfg.conn_method  = prefs.getString("conn_method", "device_network");
  cfg.wifi_ssid    = prefs.getString("wifi_ssid", "");
  cfg.wifi_pass    = prefs.getString("wifi_pass", "");
}

// ---------- JSON helpers ----------
static inline bool validF(float v) { return isfinite(v); }
static inline float clampF(float v, float lo, float hi) {
  if (!validF(v)) return NAN;
  if (v < lo) v = lo; if (v > hi) v = hi; return v;
}
static inline void setOrNullF(JsonObject obj, const char* key, float value, bool ok) {
  if (ok) obj[key] = value; else obj[key] = nullptr;
}
static inline void setOrNullI(JsonObject obj, const char* key, int value, bool ok) {
  if (ok) obj[key] = value; else obj[key] = nullptr;
}
static inline bool validCoord(float lat, float lon) {
  return validF(lat) && validF(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

// ================== SENSORS =================
OneWireNg_CurrentPlatform oneWire(ESP_TEMP, false);
DSTherm tempSensor(oneWire);

float getBatteryVoltage() {
  uint32_t sum=0; for (int i=0;i<10;i++) sum += analogReadMilliVolts(ESP_ADC_VBAT);
  float avg_mV = sum/10.0f; return (avg_mV/1000.0f)*DIVIDER_RATIO;
}
float getBatteryPercent() {
  float v = getBatteryVoltage();
  float pct = 100.0f * (v - BATTERY_VOLTAGE_MIN)/(BATTERY_VOLTAGE_MAX - BATTERY_VOLTAGE_MIN);
  return constrain(pct, 0.0f, 100.0f);
}
float getMoisture_mV() {
  uint32_t sum=0; for (int i=0;i<10;i++) sum += analogReadMilliVolts(ESP_ADC_MOIST);
  return sum/10.0f;
}
float getSoilMoisturePercent() {
  float mv = getMoisture_mV();
  float pct = 100.0f * (MOISTURE_MV_DRY - mv)/(MOISTURE_MV_DRY - MOISTURE_MV_WET);
  return constrain(pct, 0.0f, 100.0f);
}
float getResistance() {
  uint32_t sum_vcc=0,sum_sen=0,sum_gnd=0;
  for(int i=0;i<10;i++){ sum_vcc+=analogReadMilliVolts(ESP_ADC_VCC); sum_sen+=analogReadMilliVolts(ESP_ADC_SEN); sum_gnd+=analogReadMilliVolts(ESP_ADC_GND); }
  float v_vcc = sum_vcc/10.0f, v_sen=sum_sen/10.0f, v_gnd=sum_gnd/10.0f;
  float current_mA = (v_vcc - v_sen)/R_KNOWN; if (current_mA<=0.000001f) return -1.0f;
  float sensor_v = v_sen - v_gnd; return sensor_v/current_mA;
}

// -------- SCD4x Robust bring-up & read --------
static bool i2cProbe(uint8_t addr) {
  Wire.beginTransmission(addr);
  return (Wire.endTransmission() == 0);
}
static void scdStop() {
  int16_t e = scd4x.stopPeriodicMeasurement();
  if (!e) { g_scd_running = false; LOG("[SCD4x] stop -> OK\n"); }
}
static bool scdStart() {
  int16_t e = scd4x.startPeriodicMeasurement();
  g_scd_running = (e == 0);
  if (!e) LOG("[SCD4x] start -> OK (first sample ~5s)\n");
  else    LOG("[SCD4x] start ERR=0x%04X\n", (uint16_t)e);
  return (e == 0);
}
static bool scdInitRobust() {
  // Power rail must already be HIGH
  Wire.begin(ESP_SDA, ESP_SCL);
  Wire.setClock(I2C_SPEED_HZ);
  delay(10);

  g_scd_present = i2cProbe(SCD4X_ADDR);
  LOG("[I2C] Probe 0x%02X: %s\n", SCD4X_ADDR, g_scd_present?"FOUND":"NOT FOUND");
  if (!g_scd_present) return false;

  scd4x.begin(Wire, SCD4X_ADDR);
  delay(5);

  int16_t e = 0;
  e = scd4x.stopPeriodicMeasurement(); if (e) LOG("[SCD4x] stop ERR=0x%04X\n", (uint16_t)e);
  delay(2);
  e = scd4x.reinit();                  if (e) LOG("[SCD4x] reinit ERR=0x%04X\n", (uint16_t)e);
  delay(20);

  uint64_t sn = 0;
  if (!scd4x.getSerialNumber(sn)) {
    LOG("[SCD4x] Serial: 0x%016llX\n", (unsigned long long)sn);
  }

  g_scd_fail_streak = 0;
  return scdStart();
}
static bool scdWaitFirstSample(uint32_t timeout_ms=6500) {
  if (!g_scd_present || !g_scd_running) return false;
  uint32_t t0 = millis();
  while (millis() - t0 < timeout_ms) {
    uint16_t co2=0; float t=NAN, rh=NAN;
    int16_t e = scd4x.readMeasurement(co2, t, rh);
    if (!e && co2 != 0) {
      scd_co2_ppm = co2; scd_temp_c = t; scd_rh = rh; return true;
    }
    delay(250);
  }
  return false;
}
static void scdReadLatest() {
  if (!g_scd_present || !g_scd_running) return;
  uint16_t co2=0; float t=NAN, rh=NAN;
  int16_t e = scd4x.readMeasurement(co2, t, rh);
  if (!e && co2 != 0) {
    g_scd_fail_streak = 0;
    scd_co2_ppm = co2; scd_temp_c = t; scd_rh = rh;
  } else if (e) {
    g_scd_fail_streak++;
    LOG("[SCD4x] read ERR=0x%04X (streak=%u)\n", (uint16_t)e, g_scd_fail_streak);
    if (g_scd_fail_streak >= 3) {
      LOG("[SCD4x] Reinit after 3 fails\n");
      scdStop(); delay(5);
      scd4x.reinit(); delay(20);
      scdStart();
      g_scd_fail_streak = 0;
    }
  }
}

float getSoilTempC() {
  Placeholder<DSTherm::Scratchpad> scrpd;
  tempSensor.convertTempAll(94, false); // 9-bit
  for (const auto& id : oneWire) {
    if (DSTherm::getFamilyName(id)) {
      if (tempSensor.readScratchpad(id, scrpd) == OneWireNg::EC_SUCCESS) {
        return scrpd->getTemp2()/16.0f;
      }
    }
  }
  return -1.0f;
}

void gatherSensorData(SensorData_t &d) {
  memset(&d, 0, sizeof(d));
  d.resistance          = getResistance();
  d.batteryPercent      = getBatteryPercent();
  d.soilMoisturePercent = getSoilMoisturePercent();
  d.soilTempC           = getSoilTempC();

  // SCD4x: we should have waited once already; take one more read tick
  scdReadLatest();
  d.co2PPM              = scd_co2_ppm;
  d.airTempC            = scd_temp_c;
  d.airHumidity         = scd_rh;

  d.latitude  = cfg.lat.length()? atof(cfg.lat.c_str()) : NAN;
  d.longitude = cfg.lon.length()? atof(cfg.lon.c_str()) : NAN;
  snprintf(d.nodeName, sizeof(d.nodeName), "%s", cfg.nickname.c_str());
}

// ================== HUB PATH =================
static void macToStr(const uint8_t *mac, char out[18]) {
  sprintf(out,"%02X:%02X:%02X:%02X:%02X:%02X",mac[0],mac[1],mac[2],mac[3],mac[4],mac[5]);
}
void onDataSent(const uint8_t *, esp_now_send_status_t status) {
  LOG("[ESP-NOW] Send %s\n", status==ESP_NOW_SEND_SUCCESS?"OK":"FAIL");
}
bool addOrUpdatePeer(const uint8_t *addr, uint8_t channel) {
  esp_now_peer_info_t peer{}; memcpy(peer.peer_addr, addr, 6);
  peer.channel=channel; peer.ifidx=WIFI_IF_STA; peer.encrypt=false;
  if (esp_now_is_peer_exist(addr)) esp_now_del_peer(addr);
  return esp_now_add_peer(&peer)==ESP_OK;
}
bool initEspNowTx() {
  WiFi.mode(WIFI_STA);
  esp_wifi_set_promiscuous(true);
  esp_wifi_set_channel(ESPNOW_CHANNEL, WIFI_SECOND_CHAN_NONE);
  esp_wifi_set_promiscuous(false);
  if (esp_now_init()!=ESP_OK) return false;
  esp_now_register_send_cb(onDataSent);
  return addOrUpdatePeer(hubAddress, ESPNOW_CHANNEL);
}
bool sendSensorDataViaHubOnce() {
  if (!initEspNowTx()) { LOG("ESP-NOW init fail\n"); return false; }
  SensorData_t data{}; gatherSensorData(data);
  esp_err_t r = esp_now_send(hubAddress, (uint8_t*)&data, sizeof(data));
  uint32_t t0=millis(); while (millis()-t0<200) delay(5);
  esp_now_deinit();
  return (r==ESP_OK);
}

// ================== CLOUD PATH =================
static String urlEncode(const String& s) {
  String out; out.reserve(s.length()*3);
  for (size_t i=0;i<s.length();i++){
    char c=s[i];
    bool unres = (c>='A'&&c<='Z')||(c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-'||c=='_'||c=='.'||c=='~';
    if (unres) out+=c; else { char b[4]; sprintf(b,"%%%02X",(uint8_t)c); out+=b; }
  }
  return out;
}

static String buildArcGisFeaturesJson(const SensorData_t &d) {
  const float kMaxNumeric = 99999.9f;

  auto validF_ = [](float v){ return isfinite(v); };
  auto clampF_ = [&](float v, float lo, float hi){
    if (!validF_(v)) return NAN;
    if (v < lo) v = lo; if (v > hi) v = hi; return v;
  };
  auto validCoord_ = [&](float lat, float lon){
    return validF_(lat) && validF_(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
  };

  float resistance_ohms = d.resistance;
  bool  resistance_ok   = validF_(resistance_ohms) && resistance_ohms >= 0.0f;
  if (resistance_ok && resistance_ohms > kMaxNumeric) resistance_ohms = kMaxNumeric;

  float battery_pct_f   = clampF_(d.batteryPercent, 0.0f, 100.0f);
  bool  battery_ok      = validF_(battery_pct_f);
  int   battery_pct_i   = battery_ok ? (int)roundf(battery_pct_f) : 0;

  float soil_moist_pct  = clampF_(d.soilMoisturePercent, 0.0f, 100.0f);
  bool  soil_moist_ok   = validF_(soil_moist_pct);

  float soil_temp_c     = clampF_(d.soilTempC, -50.0f, 125.0f);
  bool  soil_temp_ok    = validF_(soil_temp_c);

  float co2_ppm_f       = clampF_(d.co2PPM, 0.0f, 50000.0f);
  bool  co2_ok          = validF_(co2_ppm_f);
  int   co2_ppm_i       = co2_ok ? (int)roundf(co2_ppm_f) : 0;

  float air_temp_c      = clampF_(d.airTempC, -50.0f, 125.0f);
  bool  air_temp_ok     = validF_(air_temp_c);

  float air_hum_pct     = clampF_(d.airHumidity, 0.0f, 100.0f);
  bool  air_hum_ok      = validF_(air_hum_pct);

  float lat = clampF_(d.latitude,  -90.0f,  90.0f);
  float lon = clampF_(d.longitude, -180.0f, 180.0f);
  bool  geo_ok = validCoord_(lat, lon);

  StaticJsonDocument<1200> doc;
  JsonArray  features = doc.to<JsonArray>();
  JsonObject feature  = features.createNestedObject();
  JsonObject attrs    = feature.createNestedObject("attributes");

  attrs["node_id"] = GetNodeIdFromMac();

  setOrNullF(attrs, "resistance_ohms",       resistance_ohms,   resistance_ok);
  setOrNullF(attrs, "soil_moisture_percent", soil_moist_pct,    soil_moist_ok);
  setOrNullF(attrs, "soil_temp_c",           soil_temp_c,       soil_temp_ok);
  setOrNullI(attrs, "battery_percent",       battery_pct_i,     battery_ok);
  setOrNullI(attrs, "co2_ppm",               co2_ppm_i,         co2_ok);
  setOrNullF(attrs, "air_temp_c",            air_temp_c,        air_temp_ok);
  setOrNullF(attrs, "air_humidity_percent",  air_hum_pct,       air_hum_ok);

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

bool connectWiFiSTA(const String& ssid, const String& pass, uint32_t timeoutMs=20000) {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid.c_str(), pass.c_str());
  uint32_t t0=millis();
  while (WiFi.status()!=WL_CONNECTED && millis()-t0<timeoutMs) delay(200);
  if (WiFi.status()==WL_CONNECTED) {
    LOG("[WiFi] Connected %s | IP %s\n", ssid.c_str(), WiFi.localIP().toString().c_str());
    return true;
  }
  LOG("[WiFi] Failed to connect %s\n", ssid.c_str());
  return false;
}

bool UploadToCloudOnce() {
  if (!cfg.wifi_ssid.length()) return false;
  if (WiFi.status()!=WL_CONNECTED) {
    if (!connectWiFiSTA(cfg.wifi_ssid, cfg.wifi_pass)) return false;
  }

  SensorData_t d{}; gatherSensorData(d);
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
    LOG("[Cloud] HTTP %d\n", code);
    if (code > 0) resp = http.getString();
    http.end();
  } else {
    LOG("HTTP begin failed\n");
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
          LOG("[ArcGIS] addFeatures failed (%d): %s\n", code, desc);
        }
      }
    } else {
      LOG("JSON parse error: %s\n", derr.c_str());
    }
  }

  WiFi.disconnect(true, true);
  return okHttp && okApi;
}

// ============== DEEP SLEEP CYCLE =============
void SleepMinutes(uint32_t minutes) {
  uint64_t us = (uint64_t)minutes * 60ULL * 1000000ULL;
  LOG("[Sleep] %lu min -> %llu us\n", (unsigned long)minutes, us);
  esp_sleep_enable_timer_wakeup(us);
  esp_deep_sleep_start();
}

// ============== MODE: WIZARD OR RUN ==========
bool bootButtonHeld() {
  pinMode(BOOT_BTN, INPUT_PULLUP);
  delay(10);
  return digitalRead(BOOT_BTN)==LOW;
}
bool needsWizard() {
  if (bootButtonHeld()) return true;
  if (cfg.conn_method=="direct_wifi" && !cfg.wifi_ssid.length()) return true;
  if (!cfg.lat.length() || !cfg.lon.length()) return true;
  return false;
}

// ============== ARDUINO ======================
void setup() {
  Serial.begin(115200);
  delay(150);
  LOG("\n[DirtData] Boot\n");

  // Power rails & ADC
  pinMode(ESP_PWR_3V3, OUTPUT); digitalWrite(ESP_PWR_3V3, LOW); // keep sensors OFF until run mode
  pinMode(ESP_GPS_3V3, OUTPUT); digitalWrite(ESP_GPS_3V3, LOW);

  analogReadResolution(12);
  analogSetAttenuation(ADC_11db);
  pinMode(ESP_ADC_VCC,   INPUT);
  pinMode(ESP_ADC_SEN,   INPUT);
  pinMode(ESP_ADC_GND,   INPUT);
  pinMode(ESP_ADC_VBAT,  INPUT);
  pinMode(ESP_ADC_MOIST, INPUT);
  pinMode(ESP_TEMP, INPUT);

  // Load persisted config first
  loadPrefs();

  // ---- Setup Wizard mode (no I2C/sensors) ----
  if (needsWizard()) {
    startAP(); startDNS(); startHTTP();
    while (true) { dns.processNextRequest(); server.handleClient(); delay(5); }
  }

  // ---- Operational one-shot: power sensors -> init -> (wait) -> send -> sleep ----
  digitalWrite(ESP_PWR_3V3, HIGH);
  delay(10);                           // rail settle
  Wire.begin(ESP_SDA, ESP_SCL);
  Wire.setClock(I2C_SPEED_HZ);
  delay(10);

  // SCD4x robust init and wait for first sample (best-effort)
  g_scd_present = scdInitRobust();
  if (g_scd_present) (void)scdWaitFirstSample(6500);

  if (cfg.conn_method=="direct_wifi") {
    bool ok = UploadToCloudOnce();
    LOG("[Run] Direct Wi-Fi upload: %s\n", ok?"OK":"FAIL");
  } else {
    if (!initEspNowTx()) { LOG("ESP-NOW init fail\n"); }
    else {
      SensorData_t pkt{}; gatherSensorData(pkt);
      esp_now_send(hubAddress, (uint8_t*)&pkt, sizeof(pkt));
      LOG("[Run] ESP-NOW send queued\n");
      esp_now_deinit();
    }
  }

  // Power down sensor rail before sleep
  digitalWrite(ESP_PWR_3V3, LOW);

  // Deep sleep until next interval
  SleepMinutes(cfg.interval_min ? cfg.interval_min : 30);
}

void loop() {
  // not used
}
