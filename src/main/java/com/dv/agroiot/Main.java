package com.dv.agroiot;

public class Main {

    public static void main(String[] args) {

        System.out.println("Iniciando SUSCRIPTOR...");

        Thread subThread = new Thread(() -> {
            try {
                new Suscriptor().start();
            } catch (Exception e) {
                System.out.println("[ERROR] Suscriptor falló: " + e.getMessage());
                e.printStackTrace();
            }
        }, "mqtt-suscriptor");

        // ✅ Mantener activo el suscriptor
        subThread.start();

        // ⛔ SIMULADOR DESACTIVADO (para usar datos reales del MQTT)
        /*
        try {
            Thread.sleep(800); // pequeño margen
        } catch (InterruptedException ignored) {}

        System.out.println("Iniciando SIMULADOR...");

        try {
            new SimuladorPublisher().start(); // este corre infinito
        } catch (Exception e) {
            System.out.println("[ERROR] SimuladorPublisher falló: " + e.getMessage());
            e.printStackTrace();
        }
        */
    }
}