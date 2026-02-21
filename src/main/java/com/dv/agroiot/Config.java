package com.dv.agroiot;

public class Config {
    // Cambia esto por el broker que te pasen
    public static final String MQTT_BROKER = "tcp://test.mosquitto.org:1883";
    public static final String MQTT_USER = ""; // si no aplica, déjalo ""
    public static final String MQTT_PASS = "";
    // --- MariaDB (local) ---
public static final String DB_URL  = "jdbc:mariadb://127.0.0.1:3306/agro_iot";
public static final String DB_USER = "root";
public static final String DB_PASS = "pucmm";

    // Publicaremos aquí:
    // agro_iot/EST-001/mediciones
    public static final String TOPIC_BASE = "agro_iot";
    public static final String[] ESTACIONES = {"EST-001", "EST-002"};

    // cada cuánto publica el simulador
    public static final int INTERVAL_MS = 2000;
}