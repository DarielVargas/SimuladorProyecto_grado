package com.dv.agroiot;

public class Config {

    // --- MQTT (Broker público HiveMQ) ---
    public static final String MQTT_BROKER = "tcp://broker.hivemq.com:1883";
    public static final String MQTT_USER = "";
    public static final String MQTT_PASS = "";

    // IMPORTANTE: todo debe colgar de este topic
    // Ejemplo final: /20181853/EST-001/mediciones
    public static final String TOPIC_BASE = "/20181853";

    public static final String[] ESTACIONES = {"EST-001", "EST-002"};

    // --- MariaDB (local) ---
    public static final String DB_URL  = "jdbc:mariadb://127.0.0.1:3306/agro_iot";
    public static final String DB_USER = "root";
    public static final String DB_PASS = "pucmm";

    // Cada cuánto publica el simulador
    public static final int INTERVAL_MS = 2000;
}