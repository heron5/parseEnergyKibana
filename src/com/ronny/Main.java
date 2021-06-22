package com.ronny;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.elasticsearch.client.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;


import org.apache.http.HttpHost;

public class Main {

    public Double meterAck(String source, double power) {
//  meterAck function. The import/export-meter (10 & 11) seems to have a bad counting for kWh.
//                     We now compute the consumption as power/60 -> kWh last minute and add to ackumulator.

        if (source.equals("10")) {
            if (power < 0)
                power = 0;
        }
        if (source.equals("11")) {
            if (power > 0)
                power = 0;
        }
        power = Math.abs(power);
        String ackValue = "0";
        try {
            //File currentPMFile = new File("powermeter_" + source + ".txt");
            File currentPMFile = new File("/usr/local/kjula/powermeter_" + source + ".txt");
            Scanner fileReader = new Scanner(currentPMFile);
            while (fileReader.hasNextLine()) {
                ackValue = fileReader.nextLine();
                System.out.println(ackValue);
            }
            fileReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred while reading powermeter file.");
            e.printStackTrace();
        }
        Double consumption = (Double) ((power / 60.0) / 1000.0);
        Double newAckValue = Double.parseDouble(ackValue) + consumption;

        try {
            // FileWriter updatedPMFile = new FileWriter("powermeter_" + source + ".txt", false);
            FileWriter updatedPMFile = new FileWriter("/usr/local/kjula/powermeter_" + source + ".txt", false);
            updatedPMFile.write(newAckValue + "\n");
            updatedPMFile.close();
        } catch (IOException e) {
            System.err.print("FileWriter went wrong while writing powermeter file.");
            e.printStackTrace();
        }

        return newAckValue;
    }

    public Double dailyYield(String source, Double totalKWh){
//  dailyYield function. At midnight - or in fact at first run of the day - we are saving the current total to file.
//                       This is then used during the day to calculate the current daily consumption.

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd" );
        LocalDateTime now = LocalDateTime.now();

        try {
           // File outputDateFile = new File("DailyInit_" + source + "_" + dtf.format(now));
            File outputDateFile = new File("/usr/local/kjula/dailyinit/DailyInit_" + source + "_" + dtf.format(now));

            if (outputDateFile.createNewFile()) {
                System.out.println("File created: " + outputDateFile.getName());
                FileWriter writer = new FileWriter(outputDateFile);
                writer.write(totalKWh + "\n");
                writer.close();
            }
        } catch (IOException e) {
            System.out.println("An error occurred: outputDateFile");
            e.printStackTrace();
        }
        String dayInit = "0";
        try {
            // File inputDateFile = new File("DailyInit_" + source + "_" + dtf.format(now));
            File inputDateFile = new File("/usr/local/kjula/dailyinit/DailyInit_" + source + "_" + dtf.format(now));
            Scanner fileReader = new Scanner(inputDateFile);
            while (fileReader.hasNextLine()) {
                dayInit = fileReader.nextLine();
              //  System.out.println(dayInit);
            }
            fileReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred while reading inputDateFile file.");
            e.printStackTrace();
        }

        Double yield = totalKWh - Double.parseDouble(dayInit);

        return yield;
    }

    public void updateDb(String source, Double power, Double meterKWh, Double todayKWh,
                         String timeOffset, String host, String port, int loggLevel) {

        SensorProperties sensor = new SensorProperties();
        if (!sensor.getSensorProperties(source))
            return;

        lookupSpotPrice spotPrice;
        Double price = 0.00;
        Double averagePrice = 0.00;
        try {
            spotPrice = new lookupSpotPrice();
            spotPrice.lookupSpot();
            price = spotPrice.price;
            averagePrice = spotPrice.averagePrice;
        }catch (Exception e){
            System.out.println(e);
        }

        long sourceId = sensor.sourceId;
        String description = sensor.shortDescription;
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss" + timeOffset.trim());
        LocalDateTime now = LocalDateTime.now();

        JSONObject jsonBody = new JSONObject();
        JSONObject jsonTags = new JSONObject();
        JSONObject jsonFields = new JSONObject();

        jsonTags.put("source", sourceId);
        jsonTags.put("source_desc", description);
        jsonFields.put("power", power);
        jsonFields.put("kWh", meterKWh);
        jsonFields.put("today_kWh", todayKWh);
        jsonFields.put("current_spot", price);
        jsonFields.put("average_spot", averagePrice);

        jsonBody.put("measurement", "EnergyLogg");
        jsonBody.put("date", dtf.format(now));
        jsonBody.put("tags", jsonTags);
        jsonBody.put("fields", jsonFields);

        if (loggLevel > 1)
            System.out.println(jsonBody);

        try {
            FileWriter jsonFile = new FileWriter("/tmp/energy_consumed_"+source+".json", false);
            jsonFile.write(jsonBody + "\n");
            jsonFile.close();
        } catch (IOException e) {
            System.err.print("FileWriter went wrong");
            e.printStackTrace();
        }

        HttpHost esHost = new HttpHost(host, Integer.parseInt(port));
        RestClient restClient = RestClient.builder(esHost).build();

        Request request = new Request(
                "POST",
                "/energy_index/energylog");

        request.setJsonEntity(jsonBody.toString());

        Response response = null;
        try {
            response = restClient.performRequest(request);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                restClient.close();
            } catch (IOException closeEx) {
                closeEx.printStackTrace();
            }

            if (loggLevel > 0)
                System.out.println(response);

        }
    }


    public void run() {
        System.out.println("TopicSubscriber initializing...");
        System.out.println("Get properties from file...");

        String host = "";
        String username = "";
        String password = "";
        String topic = "";
        String timeOffset = "";
        String kibanaHost = "";
        String kibanaPort = "";
        int loggLevel = 0;


        try (InputStream input = new FileInputStream("parseEnergyKibana.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            host = prop.getProperty("mqttHost");
            username = prop.getProperty("mqttUsername");
            password = prop.getProperty("mqttPassword");
            topic = prop.getProperty("mqttTopic");
            timeOffset = prop.getProperty("timeOffset");
            kibanaHost = prop.getProperty("kibanaHost");
            kibanaPort = prop.getProperty("kibanaPort");
            loggLevel = Integer.parseInt(prop.getProperty("loggLevel"));
            System.out.println("Logglevel: " + loggLevel);

        } catch (IOException ex) {
            ex.printStackTrace();
        }


        try {
            // Create an Mqtt client
            Random rand = new Random();
            MqttClient mqttClient = new MqttClient(host, "parseEnergy" + rand.toString());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
            connOpts.setAutomaticReconnect(true);

            // Connect the client
            System.out.println("Connecting to Kjuladata messaging at " + host);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            // Topic filter the client will subscribe to
            final String subTopic = topic;

            // Callback - Anonymous inner-class for receiving messages
            String finalTimeOffset = timeOffset;
            String finalKibanaHost = kibanaHost;
            String finalKibanaPort = kibanaPort;
            int finalLoggLevel = loggLevel;
            mqttClient.setCallback(new MqttCallback() {
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // Called when a message arrives from the server that
                    // matches any subscription made by the client
                    String payLoad = new String(message.getPayload());
                    // Check if logging is from Vilan or other sources.
                    // Vilan has the old query-string format.

                    if (topic.equals("vilan/energy/tot/update")) {
                        System.out.println(topic);
                        System.out.println(payLoad);
                        String[] parts = payLoad.split("&");
                        String effekt = parts[0]; // effekt
                        String max = parts[1];  // max
                        String min = parts[2];  // min
                        String puls = parts[3]; // puls

                        String[] effektParts = effekt.split("=");
                        Double vilanPower = Double.parseDouble(effektParts[1]);
                        String[] pulsParts = puls.split("=");
                        Double vilanPuls = Double.parseDouble(pulsParts[1]);

                        System.out.println(vilanPower);
                        System.out.println(vilanPuls);
                        updateDb("6", vilanPower, vilanPuls, 0.0, finalTimeOffset,
                                finalKibanaHost, finalKibanaPort, finalLoggLevel);
                        
                    }       else {

                        JSONParser parser = new JSONParser();
                        // String payLoad = new String(message.getPayload());
                        JSONObject json = (JSONObject) parser.parse(payLoad);
                        JSONArray energylogg = (JSONArray) json.get("energylogg");

                        if (finalLoggLevel > 1)
                            System.out.println(topic);
                        System.out.println(payLoad);

                        try {
                            for (Object logg : energylogg) {
                                JSONObject loggbysource = (JSONObject) logg;
                                String source = (String) loggbysource.get("source");
                                double meterKWh = Double.parseDouble((String) loggbysource.get("kwh"));
                                double power = Double.parseDouble((String) loggbysource.get("power"));

                                if (source.equals("10") || source.equals("11"))
                                    meterKWh = meterAck(source, power);
                                Double todayKWh = dailyYield(source, meterKWh);
                                updateDb(source, power, meterKWh, todayKWh, finalTimeOffset,
                                        finalKibanaHost, finalKibanaPort, finalLoggLevel);
                            }
                        } catch (Exception pe) {
                            //  System.out.println("position: " + pe.getPosition());
                            System.out.println(pe);
                        }
                    }
                }

                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to KjulaData messaging lost!" + cause.getMessage());
                    System.exit(0);
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }

            });

            // Subscribe client to the topic filter and a QoS level of 0
            System.out.println("Subscribing client to topic: " + subTopic);
            mqttClient.subscribe(subTopic, 0);
            System.out.println("Subscribed");

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Main().run();

    }
}
