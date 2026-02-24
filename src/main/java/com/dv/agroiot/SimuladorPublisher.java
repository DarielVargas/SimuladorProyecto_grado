package com.dv.agroiot;

import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class SimuladorPublisher {

    private MqttClient client;
    private final Random rnd = new Random();
    private final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void start() throws Exception {
        client = new MqttClient(Config.MQTT_BROKER, MqttClient.generateClientId());

        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);

        // ✅ Credenciales del profesor (vienen de Config)
        if (Config.MQTT_USER != null && !Config.MQTT_USER.isBlank()) {
            opt.setUserName(Config.MQTT_USER);
            opt.setPassword(Config.MQTT_PASS.toCharArray());
        }

        client.connect(opt);
        System.out.println("Conectado a MQTT: " + Config.MQTT_BROKER);

        int idx = 0;

        while (true) {
            // ✅ Alterna estaciones: EST-001, EST-002, ...
            String estacion = Config.ESTACIONES[idx % Config.ESTACIONES.length];
            idx++;

            // ✅ Topic por estación (RECOMENDADO)
            // Ej: /20181853/EST-001/mediciones
            String topic = Config.TOPIC_BASE + "/" + estacion + "/mediciones";

            String payload = construirPayload(estacion);
            publicar(topic, payload);

            Thread.sleep(Config.INTERVAL_MS);
        }
    }

    private String construirPayload(String estacion) {
        String fecha = LocalDateTime.now().format(fmt);

        // Array de mediciones mínimo: t (tipo) y v (valor)
        java.util.List<java.util.Map<String, Object>> m = new java.util.ArrayList<>();

        // 1..9 según tu tabla tipo_sensor
        m.add(mapTV(1, round2(clamp(55 + rnd.nextGaussian() * 10, 0, 100))));           // Humedad Suelo %
        m.add(mapTV(2, round2(clamp(27 + rnd.nextGaussian() * 2.5, 15, 35))));          // Temp Ambiental °C
        m.add(mapTV(3, round2(clamp(65 + rnd.nextGaussian() * 12, 20, 90))));           // Humedad Ambiental %
        m.add(mapTV(4, round2(clamp(40000 + rnd.nextGaussian() * 15000, 0, 120000))));  // Luz Lux
        m.add(mapTV(5, round2(clamp(6.5 + rnd.nextGaussian() * 0.4, 0, 14))));          // pH
        m.add(mapTV(6, round2(clamp(1.8 + rnd.nextGaussian() * 0.6, 0, 10))));          // Conductividad dS/m
        m.add(mapTV(7, round2(clamp(180 + rnd.nextGaussian() * 40, 0, 500))));          // N mg/kg
        m.add(mapTV(8, round2(clamp(60 + rnd.nextGaussian() * 20, 0, 300))));           // P mg/kg
        m.add(mapTV(9, round2(clamp(220 + rnd.nextGaussian() * 50, 0, 600))));          // K mg/kg

        java.util.Map<String, Object> root = new java.util.LinkedHashMap<>();
        root.put("estacion", estacion);
        root.put("fecha", fecha);
        root.put("m", m);

        return JsonUtil.toJson(root);
    }

    private java.util.Map<String, Object> mapTV(int tipo, double valor) {
        java.util.Map<String, Object> x = new java.util.LinkedHashMap<>();
        x.put("t", tipo);
        x.put("v", valor);
        return x;
    }

    private void publicar(String topic, String payload) throws Exception {
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        client.publish(topic, msg);
        System.out.println("[PUB] " + topic + " -> " + payload);
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    private double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}