package com.dv.agroiot;

public class Config {

    // --- MQTT (Servidor del profesor) ---
    public static final String MQTT_BROKER = "tcp://mqtt.eict.ce.pucmm.edu.do:1883";
    public static final String MQTT_USER = "20181853";
    public static final String MQTT_PASS = "qqLKTrZJPC82";

    // IMPORTANTE: todo debe colgar de este topic
    // Ejemplo final: /20181853/EST-001/mediciones
    public static final String TOPIC_BASE = "/20181853";

    public static final String[] ESTACIONES = {"EST-001", "EST-002"};

    // --- MariaDB (local) ---
    public static final String DB_URL  = "jdbc:mariadb://127.0.0.1:3306/agro_iot";
    public static final String DB_USER = "root";
    public static final String DB_PASS = "pucmm";

    // cada cuánto publica el simulador
    public static final int INTERVAL_MS = 2000;
}