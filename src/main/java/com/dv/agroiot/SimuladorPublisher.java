package com.dv.agroiot;

import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class SimuladorPublisher {

    private MqttClient client;
    private final Random rnd = new Random();

    // Formato que espera tu suscriptor actual
    private final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy");

    public void start() throws Exception {
        client = new MqttClient(Config.MQTT_BROKER, MqttClient.generateClientId());

        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);

        if (Config.MQTT_USER != null && !Config.MQTT_USER.isBlank()) {
            opt.setUserName(Config.MQTT_USER);
            opt.setPassword(Config.MQTT_PASS.toCharArray());
        }

        client.connect(opt);
        System.out.println("Conectado a MQTT: " + Config.MQTT_BROKER);

        int idx = 0;

        while (true) {
            String estacion = Config.ESTACIONES[idx % Config.ESTACIONES.length];
            idx++;

            // Topic que espera tu suscriptor actual
            String topic = Config.TOPIC_BASE + "/mediciones";

            String payload = construirPayload(estacion);
            publicar(topic, payload);

            Thread.sleep(Config.INTERVAL_MS);
        }
    }

    private String construirPayload(String estacion) {
        String fecha = LocalDateTime.now().format(fmt);

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("estacion", estacion);
        root.put("fecha", fecha);

        // Campos que espera tu suscriptor actual
        root.put("humedad_suelo", round2(clamp(55 + rnd.nextGaussian() * 10, 0, 100)));
        root.put("temperatura_ambiental", round2(clamp(27 + rnd.nextGaussian() * 2.5, 15, 35)));
        root.put("humedad_ambiental", round2(clamp(65 + rnd.nextGaussian() * 12, 20, 90)));
        root.put("luz", round2(clamp(40000 + rnd.nextGaussian() * 15000, 0, 120000)));
        root.put("ph_suelo", round2(clamp(6.5 + rnd.nextGaussian() * 0.4, 0, 14)));
        root.put("conductividad", round2(clamp(1.8 + rnd.nextGaussian() * 0.6, 0, 10)));
        root.put("nitrogeno", round2(clamp(180 + rnd.nextGaussian() * 40, 0, 500)));
        root.put("fosforo", round2(clamp(60 + rnd.nextGaussian() * 20, 0, 300)));
        root.put("potasio", round2(clamp(220 + rnd.nextGaussian() * 50, 0, 600)));

        // Opcional: tu suscriptor la muestra, pero no la guarda
        root.put("senal", round2(clamp(-55 + rnd.nextGaussian() * 8, -120, 0)));

        return JsonUtil.toJson(root);
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