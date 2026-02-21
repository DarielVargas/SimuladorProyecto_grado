package com.dv.agroiot;

public class Main {

    public static void main(String[] args) throws Exception {

        System.out.println("Iniciando SUSCRIPTOR...");

        Thread t1 = new Thread(() -> {
            try {
                new Suscriptor().start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t1.setDaemon(true);
        t1.start();

        Thread.sleep(800); // pequeño margen

        System.out.println("Iniciando SIMULADOR...");
        new SimuladorPublisher().start();
    }
}