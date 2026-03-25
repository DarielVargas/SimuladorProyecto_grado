package com.dv.agroiot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Suscriptor {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Db db = new Db();

    // Nombres bonitos para los tipos 1..9
    private static final Map<Integer, String> TIPO_NOMBRE = new LinkedHashMap<>();
    static {
        TIPO_NOMBRE.put(1, "Humedad Suelo (%)");
        TIPO_NOMBRE.put(2, "Temp Ambiental (°C)");
        TIPO_NOMBRE.put(3, "Humedad Ambiental (%)");
        TIPO_NOMBRE.put(4, "Luz (Lux)");
        TIPO_NOMBRE.put(5, "pH Suelo");
        TIPO_NOMBRE.put(6, "Conductividad (dS/m)");
        TIPO_NOMBRE.put(7, "N (mg/kg)");
        TIPO_NOMBRE.put(8, "P (mg/kg)");
        TIPO_NOMBRE.put(9, "K (mg/kg)");
    }

    // Formato de fecha que está enviando tu compañero
    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy");

    public void start() throws Exception {

        db.connect();

        // Tu compañero publica aquí
        String topic = Config.TOPIC_BASE + "/mediciones";

        MqttClient client = new MqttClient(
                Config.MQTT_BROKER,
                MqttClient.generateClientId()
        );

        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);

        if (Config.MQTT_USER != null && !Config.MQTT_USER.isBlank()) {
            opt.setUserName(Config.MQTT_USER);
            opt.setPassword(Config.MQTT_PASS.toCharArray());
        }

        client.connect(opt);
        System.out.println("Suscriptor conectado a MQTT");
        System.out.println("Escuchando topic: " + topic);

        client.subscribe(topic, (t, msg) -> {
            String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);

            try {
                System.out.println("\n📩 MENSAJE RECIBIDO -> " + payload);

                JsonNode root = mapper.readTree(payload);

                JsonNode estacionNode = root.get("estacion");
                JsonNode fechaNode = root.get("fecha");

                if (estacionNode == null || estacionNode.asText().isBlank()) {
                    System.out.println("[WARN] Mensaje sin 'estacion'");
                    return;
                }

                if (fechaNode == null || fechaNode.asText().isBlank()) {
                    System.out.println("[WARN] Mensaje sin 'fecha'");
                    return;
                }

                String estacionCodigo = estacionNode.asText();
                String fechaStr = fechaNode.asText();
                LocalDateTime fecha = LocalDateTime.parse(fechaStr, FMT);

                Map<Integer, Double> tipoToValor = new HashMap<>();

                // Mapeo de campos del JSON a tus tipos actuales de BD
                if (root.has("humedad_suelo") && !root.get("humedad_suelo").isNull()) {
                    tipoToValor.put(1, root.get("humedad_suelo").asDouble());
                }

                if (root.has("temperatura_ambiental") && !root.get("temperatura_ambiental").isNull()) {
                    tipoToValor.put(2, root.get("temperatura_ambiental").asDouble());
                }

                if (root.has("humedad_ambiental") && !root.get("humedad_ambiental").isNull()) {
                    tipoToValor.put(3, root.get("humedad_ambiental").asDouble());
                }

                if (root.has("luz") && !root.get("luz").isNull()) {
                    tipoToValor.put(4, root.get("luz").asDouble());
                }

                if (root.has("ph_suelo") && !root.get("ph_suelo").isNull()) {
                    tipoToValor.put(5, root.get("ph_suelo").asDouble());
                }

                if (root.has("conductividad") && !root.get("conductividad").isNull()) {
                    tipoToValor.put(6, root.get("conductividad").asDouble());
                }

                if (root.has("nitrogeno") && !root.get("nitrogeno").isNull()) {
                    tipoToValor.put(7, root.get("nitrogeno").asDouble());
                }

                if (root.has("fosforo") && !root.get("fosforo").isNull()) {
                    tipoToValor.put(8, root.get("fosforo").asDouble());
                }

                if (root.has("potasio") && !root.get("potasio").isNull()) {
                    tipoToValor.put(9, root.get("potasio").asDouble());
                }

                if (tipoToValor.isEmpty()) {
                    System.out.println("[WARN] No hay datos válidos para insertar");
                    return;
                }

                // Inserta en BD sin dañar la web
                db.insertarBatch(tipoToValor, estacionCodigo, fecha);

                System.out.println("====================================================");
                System.out.println("ESTACION: " + estacionCodigo);
                System.out.println("TOPIC   : " + t);
                System.out.println("FECHA   : " + fechaStr);
                System.out.println();

                for (Map.Entry<Integer, String> e : TIPO_NOMBRE.entrySet()) {
                    int tipo = e.getKey();
                    String nombre = e.getValue();

                    if (tipoToValor.containsKey(tipo)) {
                        System.out.printf("%-22s (T%-2d) : %.2f%n",
                                nombre,
                                tipo,
                                tipoToValor.get(tipo));
                    }
                }

                // Mostrar señal si viene, aunque no se guarde
                if (root.has("senal") && !root.get("senal").isNull()) {
                    System.out.printf("%-22s      : %.2f%n",
                            "Señal RSSI",
                            root.get("senal").asDouble());
                }

                System.out.println("====================================================");
                System.out.println("[DB OK] estacion=" + estacionCodigo + " filas=" + tipoToValor.size());

            } catch (Exception ex) {
                System.out.println("[DB ERROR] " + ex.getMessage());
                System.out.println("Topic: " + t);
                System.out.println("Payload: " + payload);
                ex.printStackTrace();
            }
        });
    }
}