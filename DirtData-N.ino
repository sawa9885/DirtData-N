/************************************************************
 * DirtData Node (ESP32-C6, Arduino core)  v1.2.0
 * ----------------------------------------------------------
 * - Any boot:
 *      * If BOOT held at boot → FACTORY RESET (clear prefs,
 *        restore defaults) then continue normal run.
 *
 * - Normal one-shot run (on each wakeup):
 *      * Power sensor rail
 *      * Init I2C, SCD4x (robust, avoid-NACK), ADS1115
 *      * WAIT for SCD4x data-ready (getDataReadyStatus +
 *        LowPowerWaitMs: delay vs light-sleep based on DEBUG)
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
 *      * If Wi-Fi enabled:
 *           - Connect Wi-Fi
 *           - Grab NTP timestamp (UTC ISO) while Wi-Fi is up
 *           - Build ArcGIS payload from SensorData_t and POST once
 *      * Log same SensorData_t as CSV row to /DirtData.csv on SD
 *        if SD logging enabled
 *
 *      * Then, if BLE is enabled:
 *           - Start runtime BLE server (config + OTA)
 *           - Advertise for a 3s window
 *           - If NO client ever connects during that window:
 *                 → Stop advertising, deep sleep for interval_min
 *           - If a client connects at any point:
 *                 → Print "waiting on app"
 *                 → Stay awake indefinitely (no sleep), keep BLE up
 *                 → Allow the app to update config, OTA, etc.
 *                 → On COMMIT write:
 *                       - Apply timestamp to RTC if provided
 *                       - Dump config
 *                       - esp_restart()
 *
 * - DEBUG flag:
 *      DEBUG=1 → Serial + delay() in waits
 *      DEBUG=0 → no Serial + light sleep in waits
 *
 * - Time source priority:
 *      1) NTP (if Wi-Fi enabled + connected)
 *      2) RTC (seeded once from BLE ISO timestamp, then free-runs)
 *      3) If neither valid → timestamp unset
 ************************************************************/

// Standard integer / size types
#include <cstdint>
#include <cstddef>

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

// Arduino core
#include <Arduino.h>

// Arduino / ESP32 libs
#include <WiFi.h>
#include <Wire.h>
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
#include "esp_system.h"

#include <SPI.h>
#include <SD.h>
#include <time.h>
#include <sys/time.h>
#include <NimBLEDevice.h>
#include <NimBLEAdvertising.h>
#include <NimBLEAdvertisementData.h>

// Make sure JsonObject is visible (ArduinoJson 6)
using ArduinoJson::JsonObject;

// ================== FIRMWARE VERSION ==================
#define FW_VERSION       "1.2.0"
#define FW_VERSION_MAJOR 1
#define FW_VERSION_MINOR 2
#define FW_VERSION_PATCH 0

static const char* GetFirmwareVersion() {
  return FW_VERSION;
}

// ================== DEBUG ==================
#define DEBUG 1
#if DEBUG
  #define LOG(...) do { Serial.printf(__VA_ARGS__); } while (0)
#else
  #define LOG(...) do {} while (0)
#endif

// Forward declarations for Arduino preprocessor weirdness
void LowPowerWaitMs(uint32_t ms);
float ReadAdsVoltage(uint8_t channel);
void RunRuntimeBleWindowOrSession();

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

  // New:
  String   timestamp_iso;        // BLE-provided ISO timestamp
  bool     sd_enable;
  bool     wifi_enable;
  bool     ble_enable;
  uint32_t ble_adv_interval_sec; // seconds (conceptual; used for logs / future)
} cfg;

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

// SD
bool g_sd_ok = false;

// BLE globals
static bool g_runtimeClientConnected      = false;
static bool g_runtimeClientEverConnected  = false;
static NimBLEServer*      g_runtimeServer      = nullptr;
static NimBLEAdvertising* g_runtimeAdvertising = nullptr;

static NimBLECharacteristic* g_charNickname   = nullptr;
static NimBLECharacteristic* g_charLat        = nullptr;
static NimBLECharacteristic* g_charLon        = nullptr;
static NimBLECharacteristic* g_charInterval   = nullptr;
static NimBLECharacteristic* g_charSsid       = nullptr;
static NimBLECharacteristic* g_charPass       = nullptr;
static NimBLECharacteristic* g_charCommit     = nullptr;
static NimBLECharacteristic* g_charFwVersion  = nullptr;
// OTA characteristics
static NimBLECharacteristic* g_charOtaControl = nullptr;
static NimBLECharacteristic* g_charOtaData    = nullptr;

// ================== RTC STATE (persists across deep sleep) =================
RTC_DATA_ATTR uint32_t g_elapsedSinceSampleSec = 0;
RTC_DATA_ATTR bool     g_hasBootedOnce        = false;


// ================== BLE UUIDs (16-bit) =================
// Services
#define UUID_SVC_CONFIG        0xA000
#define UUID_SVC_OTA           0xA100

// Config characteristics
#define UUID_CHR_NICKNAME      0xA001
#define UUID_CHR_LAT           0xA002
#define UUID_CHR_LON           0xA003
#define UUID_CHR_INTERVAL      0xA004
#define UUID_CHR_SSID          0xA005
#define UUID_CHR_PASS          0xA006
#define UUID_CHR_COMMIT        0xA007
#define UUID_CHR_TIMESTAMP     0xA008
#define UUID_CHR_SD_EN         0xA009
#define UUID_CHR_WIFI_EN       0xA00A
#define UUID_CHR_BLE_EN        0xA00B
#define UUID_CHR_ADV_INT       0xA00C
#define UUID_CHR_FW_VERSION    0xA00D

// OTA characteristics
#define UUID_CHR_OTA_CTRL      0xA101
#define UUID_CHR_OTA_DATA      0xA102

// ================== DEBUG SLEEP HELPER =================
void LowPowerWaitMs(uint32_t ms) {
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
  uint64_t mac = ESP.getEfuseMac();
  uint8_t b[6];
  for (int i = 0; i < 6; ++i) {
    b[i] = (mac >> (8 * i)) & 0xFF;
  }
  char buf[5];
  snprintf(buf, sizeof(buf), "%02X%02X", b[4], b[5]);
  return String(buf);
}

static String macStr() {
  uint64_t mac = ESP.getEfuseMac();
  uint8_t b[6];
  for (int i = 0; i < 6; ++i) {
    b[i] = (mac >> (8 * i)) & 0xFF;
  }
  char buf[18];
  snprintf(buf, sizeof(buf), "%02X:%02X:%02X:%02X:%02X:%02X",
           b[0], b[1], b[2], b[3], b[4], b[5]);
  return String(buf);
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
    LowPowerWaitMs(250);
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

float ReadAdsVoltage(uint8_t channel) {
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

  float v_3v3   = ReadAdsVoltage(0);
  float v_moist = ReadAdsVoltage(1);
  float v_micro = ReadAdsVoltage(2);
  float v_div   = ReadAdsVoltage(3);

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
  d.timestampUtc[0] = '\0';

  d.latitude  = cfg.lat.length()? atof(cfg.lat.c_str()) : NAN;
  d.longitude = cfg.lon.length()? atof(cfg.lon.c_str()) : NAN;

  d.co2PPM      = g_scd_co2;
  d.airTempC    = g_scd_temp;
  d.airHumidity = g_scd_rh;

  d.soilTempC   = soilTempC;

  bool ads_bat_valid   = validF(adsSnap.vbat_v);
  bool esp_bat_valid   = validF(espSnap.vbat_v);
  bool ads_moist_valid = validF(adsSnap.moist_v);
  bool esp_moist_valid = validF(espSnap.moist_v);
  bool ads_res_valid   = validF(adsSnap.r_ohms);
  bool esp_res_valid   = validF(espSnap.r_ohms);
  bool ads_micro_valid = validF(adsSnap.micro_v);
  bool esp_micro_valid = validF(espSnap.micro_v);

  d.batteryVoltage = ads_bat_valid ? adsSnap.vbat_v :
                     (esp_bat_valid ? espSnap.vbat_v : NAN);
  d.batteryPercent = ads_bat_valid ? adsSnap.vbat_pct :
                     (esp_bat_valid ? espSnap.vbat_pct : NAN);

  float moist_v_pref = ads_moist_valid ? adsSnap.moist_v :
                       (esp_moist_valid ? espSnap.moist_v : NAN);
  d.soilMoisture_mV     = validF(moist_v_pref) ? moist_v_pref * 1000.0f : NAN;
  d.soilMoisturePercent = ads_moist_valid ? adsSnap.moist_pct :
                          (esp_moist_valid ? espSnap.moist_pct : NAN);

  d.microVoltage = ads_micro_valid ? adsSnap.micro_v :
                   (esp_micro_valid ? espSnap.micro_v : NAN);
  d.resistance   = ads_res_valid ? adsSnap.r_ohms :
                   (esp_res_valid ? espSnap.r_ohms : NAN);

  d.ads_v3v3       = ads_v3v3;
  d.ads_moist_v    = adsSnap.moist_v;
  d.ads_micro_v    = adsSnap.micro_v;
  d.ads_vbat_div_v = ads_vbat_div;

  d.esp_vbat_mV  = esp_vbat_mV;
  d.esp_moist_mV = esp_moist_mV;
  d.esp_micro_mV = esp_micro_mV;

  snprintf(d.nodeName, sizeof(d.nodeName), "%s", cfg.nickname.c_str());
}

// ======= PAYLOAD LOGGING =========
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

  if (d.timestampUtc[0] != '\0') {
    attrs["timestamp_utc"] = d.timestampUtc;
  } else {
    attrs["timestamp_utc"] = nullptr;
  }

  setOrNullF(attrs, "battery_voltage_v",      battery_v,        battery_v_ok);
  setOrNullI(attrs, "battery_percent",        battery_pct_i,    battery_ok);
  setOrNullF(attrs, "soil_moisture_percent",  soil_moist_pct,   soil_moist_ok);
  setOrNullF(attrs, "soil_moisture_mv",       soil_moist_mv,    soil_moist_mv_ok);
  setOrNullF(attrs, "soil_temp_c",            soil_temp_c,      soil_temp_ok);
  setOrNullF(attrs, "resistance_ohms",        resistance_ohms,  resistance_ok);

  setOrNullI(attrs, "co2_ppm",                co2_ppm_i,        co2_ok);
  setOrNullF(attrs, "air_temp_c",             air_temp_c,       air_temp_ok);
  setOrNullF(attrs, "air_humidity_percent",   air_hum_pct,      air_hum_ok);

  setOrNullF(attrs, "micro_voltage_v",        micro_v,          micro_v_ok);

  setOrNullF(attrs, "ads_v3v3_v",             ads_v3v3,         validF_(ads_v3v3));
  setOrNullF(attrs, "ads_moist_v",            ads_moist_v,      validF_(ads_moist_v));
  setOrNullF(attrs, "ads_micro_v",            ads_micro_v,      validF_(ads_micro_v));
  setOrNullF(attrs, "ads_vbat_div_v",         ads_vbat_div_v,   validF_(ads_vbat_div_v));

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
  // uint32_t t0 = millis();
  // while (WiFi.status() != WL_CONNECTED && millis() - t0 < timeoutMs) {
  //   delay(200);
  // }
  return (WiFi.status() == WL_CONNECTED);
}

bool fetchTimeUTC(char *out, size_t len) {
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  struct tm timeinfo;
  const uint32_t timeoutMs = 10000;
  uint32_t start = millis();

  while (millis() - start < timeoutMs) {
    if (getLocalTime(&timeinfo, 1000)) {
      strftime(out, len, "%Y-%m-%dT%H:%M:%SZ", &timeinfo);
      LOG("[Time] NTP: %s\n", out);
      return true;
    }
  }
  LOG("[ERROR] [Time] Failed to get NTP time\n");
  if (len > 0) out[0] = '\0';
  return false;
}

bool setRtcFromIsoString(const String& iso) {
  if (!iso.length()) return false;

  int year, mon, day, hour, min, sec;
  if (sscanf(iso.c_str(), "%d-%d-%dT%d:%d:%d",
             &year, &mon, &day, &hour, &min, &sec) != 6) {
    LOG("[Time] Invalid BLE ISO timestamp: %s\n", iso.c_str());
    return false;
  }

  struct tm t{};
  t.tm_year = year - 1900;
  t.tm_mon  = mon - 1;
  t.tm_mday = day;
  t.tm_hour = hour;
  t.tm_min  = min;
  t.tm_sec  = sec;

  time_t tt = mktime(&t);
  if (tt == (time_t)-1) {
    LOG("[Time] mktime failed for BLE ISO\n");
    return false;
  }

  struct timeval tv;
  tv.tv_sec  = tt;
  tv.tv_usec = 0;
  settimeofday(&tv, nullptr);
  LOG("[Time] RTC set from BLE timestamp: %s\n", iso.c_str());
  return true;
}

bool readRtcTimeUTC(char *out, size_t len) {
  struct tm timeinfo;
  if (getLocalTime(&timeinfo, 1000)) {
    strftime(out, len, "%Y-%m-%dT%H:%M:%SZ", &timeinfo);
    LOG("[Time] RTC: %s\n", out);
    return true;
  }
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
void SleepSeconds(uint32_t seconds) {
  if (seconds == 0) seconds = 1;
  uint64_t us = (uint64_t)seconds * 1000000ULL;
  LOG("[Run] Complete, sleeping %lu s\n", (unsigned long)seconds);
  esp_sleep_enable_timer_wakeup(us);
  esp_deep_sleep_start();
}

// (optional: keep for compatibility, now just wraps SleepSeconds)
void SleepMinutes(uint32_t minutes) {
  SleepSeconds(minutes * 60UL);
}


// ================== CONFIG LOAD / FACTORY RESET =================
void loadPrefs() {
  prefs.begin("dirtdata", false);
  cfg.nickname     = prefs.getString("nickname", "");
  cfg.lat          = prefs.getString("lat", "");
  cfg.lon          = prefs.getString("lon", "");
  cfg.interval_min = prefs.getUInt("interval_min", 30);
  if (cfg.interval_min == 0) cfg.interval_min = 30;
  cfg.wifi_ssid    = prefs.getString("wifi_ssid", "");
  cfg.wifi_pass    = prefs.getString("wifi_pass", "");

  cfg.timestamp_iso       = prefs.getString("ts_iso", "");
  cfg.sd_enable           = prefs.getUChar("sd_en",   1);
  cfg.wifi_enable         = prefs.getUChar("wifi_en", 1);
  cfg.ble_enable          = prefs.getUChar("ble_en",  0);
  cfg.ble_adv_interval_sec= prefs.getUInt("ble_adv_sec", 30);
  if (cfg.ble_adv_interval_sec == 0) cfg.ble_adv_interval_sec = 30;
  prefs.end();
}

void factoryResetConfig() {
  LOG("[Factory] Resetting preferences\n");
  prefs.begin("dirtdata", false);
  prefs.clear();
  prefs.end();

  cfg.nickname = "";
  cfg.lat = "";
  cfg.lon = "";
  cfg.interval_min = 30;
  cfg.wifi_ssid = "";
  cfg.wifi_pass = "";
  cfg.timestamp_iso = "";
  cfg.sd_enable = true;
  cfg.wifi_enable = true;
  cfg.ble_enable = true;
  cfg.ble_adv_interval_sec = 30;
}

// ================== BLE CALLBACKS =================
void dumpCfgForDebug() {
  LOG("[CFG] Dump (after BLE write):\n");
  LOG("  FW version : %s\n", GetFirmwareVersion());
  LOG("  Nickname   : '%s'\n", cfg.nickname.c_str());
  LOG("  Lat        : '%s'\n", cfg.lat.c_str());
  LOG("  Lon        : '%s'\n", cfg.lon.c_str());
  LOG("  SSID       : '%s'\n", cfg.wifi_ssid.c_str());
  LOG("  Pass set   : %s\n", cfg.wifi_pass.length() ? "YES":"NO");
  LOG("  Interval   : %u min\n", (unsigned)cfg.interval_min);
  LOG("  ts_iso     : '%s'\n", cfg.timestamp_iso.c_str());
  LOG("  SD enable  : %s\n", cfg.sd_enable   ? "ON" : "OFF");
  LOG("  WiFi enable: %s\n", cfg.wifi_enable ? "ON" : "OFF");
  LOG("  BLE enable : %s\n", cfg.ble_enable  ? "ON" : "OFF");
  LOG("  BLE adv s  : %u\n", (unsigned)cfg.ble_adv_interval_sec);
}

class ConfigCharCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* c, NimBLEConnInfo& connInfo) override {
    (void)connInfo;

    std::string value = c->getValue();
    NimBLEUUID uuid = c->getUUID();

    LOG("[BLE] onWrite: char=%s, len=%u\n",
        uuid.toString().c_str(), (unsigned)value.size());

    auto makeString = [&](const std::string& v) -> String {
      char buf[96];
      size_t n = v.size();
      if (n >= sizeof(buf)) n = sizeof(buf) - 1;
      memcpy(buf, v.data(), n);
      buf[n] = '\0';
      return String(buf);
    };

    if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_NICKNAME))) {
      cfg.nickname = makeString(value);
      prefs.begin("dirtdata", false);
      prefs.putString("nickname", cfg.nickname);
      prefs.end();
      LOG("[BLE] Nickname updated: %s\n", cfg.nickname.c_str());

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_LAT))) {
      cfg.lat = makeString(value);
      prefs.begin("dirtdata", false);
      prefs.putString("lat", cfg.lat);
      prefs.end();
      LOG("[BLE] Latitude updated: %s\n", cfg.lat.c_str());

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_LON))) {
      cfg.lon = makeString(value);
      prefs.begin("dirtdata", false);
      prefs.putString("lon", cfg.lon);
      prefs.end();
      LOG("[BLE] Longitude updated: %s\n", cfg.lon.c_str());

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_INTERVAL))) {
      if (value.size() >= 4) {
        uint32_t v =
          (uint8_t)value[0] |
          ((uint8_t)value[1] << 8) |
          ((uint8_t)value[2] << 16) |
          ((uint8_t)value[3] << 24);

        if (v == 0) v = 30;
        cfg.interval_min = v;
        prefs.begin("dirtdata", false);
        prefs.putUInt("interval_min", cfg.interval_min);
        prefs.end();
        LOG("[BLE] Interval updated: %u min\n", (unsigned)cfg.interval_min);
      } else {
        LOG("[BLE] Interval write too small (%u bytes)\n", (unsigned)value.size());
      }

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_SSID))) {
      cfg.wifi_ssid = makeString(value);
      prefs.begin("dirtdata", false);
      prefs.putString("wifi_ssid", cfg.wifi_ssid);
      prefs.end();
      LOG("[BLE] WiFi SSID updated: %s\n", cfg.wifi_ssid.c_str());

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_PASS))) {
      cfg.wifi_pass = makeString(value);
      prefs.begin("dirtdata", false);
      prefs.putString("wifi_pass", cfg.wifi_pass);
      prefs.end();
      LOG("[BLE] WiFi password updated (len=%u)\n", (unsigned)cfg.wifi_pass.length());

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_COMMIT))) {
      LOG("[BLE] Commit received\n");
      if (cfg.timestamp_iso.length()) {
        setRtcFromIsoString(cfg.timestamp_iso);
      }
      dumpCfgForDebug();
      LOG("[BLE] Restarting after commit...\n");
      delay(200);
      esp_restart();

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_TIMESTAMP))) {
      cfg.timestamp_iso = makeString(value);
      prefs.begin("dirtdata", false);
      prefs.putString("ts_iso", cfg.timestamp_iso);
      prefs.end();
      LOG("[BLE] Timestamp ISO updated: %s\n", cfg.timestamp_iso.c_str());

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_SD_EN))) {
      bool en = (value.size() >= 1 && value[0] != 0);
      cfg.sd_enable = en;
      prefs.begin("dirtdata", false);
      prefs.putUChar("sd_en", en ? 1 : 0);
      prefs.end();
      LOG("[BLE] SD enable: %s\n", en ? "ON" : "OFF");

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_WIFI_EN))) {
      bool en = (value.size() >= 1 && value[0] != 0);
      cfg.wifi_enable = en;
      prefs.begin("dirtdata", false);
      prefs.putUChar("wifi_en", en ? 1 : 0);
      prefs.end();
      LOG("[BLE] WiFi enable: %s\n", en ? "ON" : "OFF");

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_BLE_EN))) {
      bool en = (value.size() >= 1 && value[0] != 0);
      cfg.ble_enable = en;
      prefs.begin("dirtdata", false);
      prefs.putUChar("ble_en", en ? 1 : 0);
      prefs.end();
      LOG("[BLE] BLE enable: %s\n", en ? "ON" : "OFF");

    } else if (uuid.equals(NimBLEUUID((uint16_t)UUID_CHR_ADV_INT))) {
      if (value.size() >= 4) {
        uint32_t v =
          (uint8_t)value[0] |
          ((uint8_t)value[1] << 8) |
          ((uint8_t)value[2] << 16) |
          ((uint8_t)value[3] << 24);

        if (v == 0) v = 30;
        cfg.ble_adv_interval_sec = v;
        prefs.begin("dirtdata", false);
        prefs.putUInt("ble_adv_sec", cfg.ble_adv_interval_sec);
        prefs.end();
        LOG("[BLE] Adv interval updated: %u s\n", (unsigned)cfg.ble_adv_interval_sec);
      } else {
        LOG("[BLE] Adv interval write too small (%u bytes)\n", (unsigned)value.size());
      }
    }
  }
};

// ================== OTA CALLBACKS =================
class OtaControlCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* c, NimBLEConnInfo& connInfo) override {
    (void)connInfo;
    std::string v = c->getValue();
    LOG("[OTA] Control write, len=%u\n", (unsigned)v.size());
    // TODO: parse control command and hook up real OTA state machine.
  }

  void onRead(NimBLECharacteristic* c, NimBLEConnInfo& connInfo) override {
    (void)connInfo;
    LOG("[OTA] Control read\n");
    // Example stub status: 0 = idle
    uint8_t status = 0;
    c->setValue(&status, 1);
  }
};

class OtaDataCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* c, NimBLEConnInfo& connInfo) override {
    (void)connInfo;
    std::string data = c->getValue();
    LOG("[OTA] Data chunk, len=%u\n", (unsigned)data.size());
    // TODO: feed 'data' bytes into your OTA chunk handler / flash writer.
  }
};

// ================== RUNTIME BLE SERVER CALLBACKS =================
class RuntimeBleServerCallbacks : public NimBLEServerCallbacks {
 public:
  void onConnect(NimBLEServer* pServer, NimBLEConnInfo& connInfo) override {
    (void)pServer;
    (void)connInfo;
    g_runtimeClientConnected = true;
    g_runtimeClientEverConnected = true;
    LOG("[BLE] Client connected, waiting on app...\n");
  }

  void onDisconnect(NimBLEServer* pServer, NimBLEConnInfo& connInfo, int reason) override {
    (void)pServer;
    (void)connInfo;
    LOG("[BLE] Client disconnected, reason=%d\n", reason);
    g_runtimeClientConnected = false;
    // Do NOT restart advertising here; our runtime loop manages adv.
  }
};

// ================== RUNTIME BLE INIT =================
void InitRuntimeBle() {
  if (g_runtimeServer != nullptr) {
    return; // already initialized
  }

  LOG("[BLE] Initializing runtime BLE server\n");
  LOG("[BLE] Free heap at start: %u bytes\n", (unsigned)esp_get_free_heap_size());

  String devName = "BioSensor-DirtDataN-" + macLast4();

  NimBLEDevice::init(devName.c_str());
  NimBLEDevice::setDeviceName(devName.c_str());
  NimBLEDevice::setPower(ESP_PWR_LVL_P9);

  g_runtimeServer = NimBLEDevice::createServer();
  g_runtimeServer->setCallbacks(new RuntimeBleServerCallbacks());

  // -------- CONFIG SERVICE --------
  LOG("[BLE] Starting config service (chars=13)\n");
  NimBLEService* cfgSvc = g_runtimeServer->createService(NimBLEUUID((uint16_t)UUID_SVC_CONFIG));
  LOG("[BLE] cfgSvc handle=%u\n", (unsigned)cfgSvc->getHandle());

  auto mkCharCfg = [&](uint16_t uuid16, uint32_t props) {
    NimBLECharacteristic* c = cfgSvc->createCharacteristic(uuid16, props);
    if (!c) {
      LOG("[BLE][CFG] ERROR creating char 0x%04X\n", uuid16);
      return (NimBLECharacteristic*)nullptr;
    }
    c->setCallbacks(new ConfigCharCallbacks());
    LOG("[BLE][CFG] char ok: 0x%04X handle=%u\n", uuid16, (unsigned)c->getHandle());
    return c;
  };

  g_charNickname = mkCharCfg(UUID_CHR_NICKNAME,
                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  g_charLat      = mkCharCfg(UUID_CHR_LAT,
                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  g_charLon      = mkCharCfg(UUID_CHR_LON,
                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  g_charInterval = mkCharCfg(UUID_CHR_INTERVAL,
                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  g_charSsid     = mkCharCfg(UUID_CHR_SSID,
                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  g_charPass     = mkCharCfg(UUID_CHR_PASS,
                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  g_charCommit   = mkCharCfg(UUID_CHR_COMMIT,
                             NIMBLE_PROPERTY::WRITE);

  NimBLECharacteristic* c_tsIso  = mkCharCfg(UUID_CHR_TIMESTAMP,
                                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  NimBLECharacteristic* c_sdEn   = mkCharCfg(UUID_CHR_SD_EN,
                                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  NimBLECharacteristic* c_wifiEn = mkCharCfg(UUID_CHR_WIFI_EN,
                                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  NimBLECharacteristic* c_bleEn  = mkCharCfg(UUID_CHR_BLE_EN,
                                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
  NimBLECharacteristic* c_bleInt = mkCharCfg(UUID_CHR_ADV_INT,
                                             NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);

  g_charFwVersion = mkCharCfg(UUID_CHR_FW_VERSION,
                              NIMBLE_PROPERTY::READ);

  if (!cfgSvc->start()) {
    LOG("[BLE] ERROR: cfgSvc->start() failed!\n");
  } else {
    LOG("[BLE] Config service started, free heap: %u bytes\n", (unsigned)esp_get_free_heap_size());
  }

  // -------- OTA SERVICE --------
  LOG("[BLE] Starting OTA service (chars=2)\n");
  NimBLEService* otaSvc = g_runtimeServer->createService(NimBLEUUID((uint16_t)UUID_SVC_OTA));
  LOG("[BLE] otaSvc handle=%u\n", (unsigned)otaSvc->getHandle());

  auto mkCharOta = [&](uint16_t uuid16, uint32_t props, NimBLECharacteristicCallbacks* cb) {
    NimBLECharacteristic* c = otaSvc->createCharacteristic(uuid16, props);
    if (!c) {
      LOG("[BLE][OTA] ERROR creating char 0x%04X\n", uuid16);
      return (NimBLECharacteristic*)nullptr;
    }
    c->setCallbacks(cb);
    LOG("[BLE][OTA] char ok: 0x%04X handle=%u\n", uuid16, (unsigned)c->getHandle());
    return c;
  };

  g_charOtaControl = mkCharOta(UUID_CHR_OTA_CTRL,
                               NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE,
                               new OtaControlCallbacks());
  {
    uint8_t status = 0; // idle
    if (g_charOtaControl) g_charOtaControl->setValue(&status, 1);
  }

  g_charOtaData = mkCharOta(UUID_CHR_OTA_DATA,
                            NIMBLE_PROPERTY::WRITE,
                            new OtaDataCallbacks());

  if (!otaSvc->start()) {
    LOG("[BLE] ERROR: otaSvc->start() failed!\n");
  } else {
    LOG("[BLE] OTA service started, free heap: %u bytes\n", (unsigned)esp_get_free_heap_size());
  }

  // ===== Populate initial values =====
  if (g_charNickname) g_charNickname->setValue(cfg.nickname.c_str());
  if (g_charLat)      g_charLat->setValue(cfg.lat.c_str());
  if (g_charLon)      g_charLon->setValue(cfg.lon.c_str());
  if (g_charInterval) {
    uint32_t v = cfg.interval_min ? cfg.interval_min : 30;
    uint8_t buf[4] = {
      (uint8_t)(v & 0xFF),
      (uint8_t)((v >> 8) & 0xFF),
      (uint8_t)((v >> 16) & 0xFF),
      (uint8_t)((v >> 24) & 0xFF)
    };
    g_charInterval->setValue(buf, 4);
  }
  if (g_charSsid) g_charSsid->setValue(cfg.wifi_ssid.c_str());
  if (g_charPass) g_charPass->setValue(cfg.wifi_pass.c_str());

  if (c_tsIso) {
    c_tsIso->setValue(cfg.timestamp_iso.c_str());
  }
  if (c_sdEn) {
    uint8_t b = cfg.sd_enable ? 1 : 0;
    c_sdEn->setValue(&b, 1);
  }
  if (c_wifiEn) {
    uint8_t b = cfg.wifi_enable ? 1 : 0;
    c_wifiEn->setValue(&b, 1);
  }
  if (c_bleEn) {
    uint8_t b = cfg.ble_enable ? 1 : 0;
    c_bleEn->setValue(&b, 1);
  }
  if (c_bleInt) {
    uint32_t v = cfg.ble_adv_interval_sec ? cfg.ble_adv_interval_sec : 30;
    uint8_t buf[4] = {
      (uint8_t)(v & 0xFF),
      (uint8_t)((v >> 8) & 0xFF),
      (uint8_t)((v >> 16) & 0xFF),
      (uint8_t)((v >> 24) & 0xFF)
    };
    c_bleInt->setValue(buf, 4);
  }

  if (g_charFwVersion) {
    g_charFwVersion->setValue(GetFirmwareVersion());
  }

  // ===== Advertising data (we control start/stop separately) =====
  NimBLEAdvertisementData advData;
  NimBLEAdvertisementData scanData;

  advData.setName(devName.c_str());
  advData.addServiceUUID(cfgSvc->getUUID());  // config service in primary adv
  scanData.addServiceUUID(otaSvc->getUUID()); // OTA service in scan response

  g_runtimeAdvertising = NimBLEDevice::getAdvertising();
  g_runtimeAdvertising->setAdvertisementData(advData);
  g_runtimeAdvertising->setScanResponseData(scanData);

  LOG("[BLE] Runtime BLE initialized as %s\n", devName.c_str());
}

// ================== RUNTIME BLE WINDOW / SESSION =================
void RunRuntimeBleWindowOrSession() {
  if (!cfg.ble_enable) {
    LOG("[BLE] Runtime BLE disabled by config\n");
    return;
  }

  InitRuntimeBle();

  g_runtimeClientConnected     = false;
  g_runtimeClientEverConnected = false;

  const uint32_t advWindowMs = 3000; // 3s advertise window when NO connection
  const uint32_t pollMs      = 100;

  LOG("[BLE] Starting runtime advertising window (3s, then sleep if no connection)\n");
  g_runtimeAdvertising->start();

  uint32_t start = millis();

  while (true) {
    // If app committed config, we reboot inside the commit handler (esp_restart)
    // so we don't need an explicit check here.

    if (g_runtimeClientEverConnected) {
      // We are in "config session" mode: stay awake, keep advertising, wait on app.
      // No timeout here; commit write will reboot.
      delay(pollMs);
      continue;
    }

    // No client has ever connected during this boot: enforce 3s window.
    if (millis() - start >= advWindowMs) {
      LOG("[BLE] Runtime adv window expired, no connection; stopping BLE\n");
      g_runtimeAdvertising->stop();
      break;
    }

    delay(pollMs);
  }
}

// ================== MODE DECISION =================
bool bootButtonHeld() {
  pinMode(BOOT_BTN, INPUT_PULLUP);
  delay(10);
  return digitalRead(BOOT_BTN)==LOW;
}

// ================== ARDUINO ======================
void setup() {
#if DEBUG
  Serial.begin(115200);
  delay(1000);
#endif

  setenv("TZ", "UTC0", 1);
  tzset();

  esp_log_level_set("i2c", ESP_LOG_NONE);

  LOG("\n[Setup] Booting\n");
  LOG("[Setup] FW version: %s\n", GetFirmwareVersion());

  pinMode(ESP_PWR_3V3, OUTPUT); digitalWrite(ESP_PWR_3V3, LOW);
  pinMode(ESP_GPS_3V3, OUTPUT); digitalWrite(ESP_GPS_3V3, LOW);

  analogReadResolution(12);
  analogSetAttenuation(ADC_11db);
  pinMode(ESP_ADC_SEN,   INPUT);
  pinMode(ESP_ADC_VBAT,  INPUT);
  pinMode(ESP_ADC_MOIST, INPUT);
  pinMode(ESP_TEMP,      INPUT);

  loadPrefs();

  // BOOT held at power-on: FACTORY RESET + continue.
  if (bootButtonHeld()) {
    LOG("[Setup] BOOT held — FACTORY RESET\n");
    factoryResetConfig();
    loadPrefs();  // reload defaults into cfg
  }

  LOG("[Setup] SD=%s WiFi=%s BLE=%s AdvInt=%u s Interval=%u min\n",
      cfg.sd_enable   ? "ON" : "OFF",
      cfg.wifi_enable ? "ON" : "OFF",
      cfg.ble_enable  ? "ON" : "OFF",
      (unsigned)cfg.ble_adv_interval_sec,
      (unsigned)cfg.interval_min);

  // If RTC time is bogus but we have a BLE timestamp, seed RTC once
  {
    time_t now = time(nullptr);
    const time_t cutoff = 1609459200L; // 2021-01-01
    if (now < cutoff && cfg.timestamp_iso.length()) {
      setRtcFromIsoString(cfg.timestamp_iso);
    }
  }

  // ====== Decide timing: BLE adv wake interval vs sampling interval ======
  uint32_t sampleSec = cfg.interval_min ? (cfg.interval_min * 60UL) : (30UL * 60UL);
  uint32_t advSec    = cfg.ble_adv_interval_sec ? cfg.ble_adv_interval_sec : 30UL;

  bool firstBootThisPower = !g_hasBootedOnce;
  g_hasBootedOnce = true;

  bool doSample = false;

  if (cfg.ble_enable) {
    // BLE enabled → wake every advSec, sample every sampleSec
    if (firstBootThisPower) {
      // Take a sample immediately on first power-up
      doSample = true;
      g_elapsedSinceSampleSec = 0;
    } else {
      g_elapsedSinceSampleSec += advSec;
      if (g_elapsedSinceSampleSec >= sampleSec) {
        doSample = true;
        g_elapsedSinceSampleSec = 0;
      }
    }
  } else {
    // BLE disabled → behave like old firmware: wake only for sampling interval
    advSec = sampleSec;
    doSample = true;
    g_elapsedSinceSampleSec = 0;
  }

  LOG("[Setup] Mode: doSample=%s, elapsedSinceSample=%u s, sampleSec=%u s, advSec=%u s (DEBUG=%d)\n",
      doSample ? "YES" : "NO",
      (unsigned)g_elapsedSinceSampleSec,
      (unsigned)sampleSec,
      (unsigned)advSec,
      DEBUG);

  // ============ SAMPLING PIPELINE (only when doSample = true) ============
  SensorData_t sample{};
  bool haveSample = false;

  if (doSample) {
    LOG("[Setup] Starting measurement\n");

    digitalWrite(ESP_PWR_3V3, HIGH);
    LOG("[PWR] Sensor rail ON\n");
    delay(20);

    // SCD4x / ADS
    g_scd_present = scdInitRobust();
    g_ads_present = adsInit();
    LowPowerWaitMs(100);

    scdWaitAndRead(6500);

    float ads_v3v3     = NAN;
    float ads_vbat_div = NAN;
    float esp_vbat_mV  = NAN;
    float esp_moist_mV = NAN;
    float esp_micro_mV = NAN;

    AnalogSnapshot_t adsSnap = readAdsSnapshot(ads_v3v3, ads_vbat_div);
    AnalogSnapshot_t espSnap = readEspSnapshot(esp_vbat_mV, esp_moist_mV, esp_micro_mV);
    float soilTempC          = getSoilTempC();

    buildSensorDataFromSnapshots(sample,
                                 adsSnap,
                                 espSnap,
                                 ads_v3v3,
                                 ads_vbat_div,
                                 esp_vbat_mV,
                                 esp_moist_mV,
                                 esp_micro_mV,
                                 soilTempC);

    bool wifi_ok = false;
    bool gotTime = false;

    if (!cfg.wifi_enable) {
      LOG("[Cloud] Wi-Fi disabled by config\n");
    } else if (!cfg.wifi_ssid.length()) {
      LOG("[ERROR] [Cloud] No Wi-Fi configured\n");
    } else {
      wifi_ok = connectWiFiSTA(cfg.wifi_ssid, cfg.wifi_pass);
      if (!wifi_ok) {
        LOG("[ERROR] [Cloud] Wi-Fi connect failed\n");
      }
    }

    if (cfg.wifi_enable && wifi_ok) {
      gotTime = fetchTimeUTC(sample.timestampUtc, sizeof(sample.timestampUtc));
      if (gotTime) {
        LOG("[Time] Using NTP timestamp\n");
      }
    }
    if (!gotTime) {
      if (readRtcTimeUTC(sample.timestampUtc, sizeof(sample.timestampUtc))) {
        LOG("[Time] Using RTC-only timestamp\n");
        gotTime = true;
      } else {
        sample.timestampUtc[0] = '\0';
        LOG("[Time] No valid time (no NTP, RTC invalid)\n");
      }
    }

    logPayload(sample);

    if (cfg.sd_enable) {
      initSD();
      sdAppendSample(sample);
    } else {
      LOG("[SD] SD logging disabled by config\n");
    }

    if (cfg.wifi_enable && wifi_ok) {
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

    // Stop SCD and power down sensor rail after sampling
    scdStop();
    delay(5);
    digitalWrite(ESP_PWR_3V3, LOW);
    LOG("[PWR] Sensor rail OFF\n");

    haveSample = true;
  } else {
    LOG("[Setup] Skipping measurement this wake (BLE-only interval)\n");
  }

  // ---- Runtime BLE window / session (only if enabled) ----
  RunRuntimeBleWindowOrSession();

  // ===== Deep sleep until next wake (BLE adv interval or sampling interval) =====
  SleepSeconds(advSec);
}

void loop() {
  // never reached
}
