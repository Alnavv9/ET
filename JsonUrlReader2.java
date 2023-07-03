package org.apache.beam.examples;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;


import org.apache.beam.examples.Recorridos;

public class JsonUrlReader2 {

    public JsonUrlReader2() {

    }

    public static void main(String[] args) throws DatabindException, MalformedURLException, IOException {
        JsonUrlReader2 j = new JsonUrlReader2();
        ArrayList<Recorridos> Atp = j.cargarURLx();

        for (Recorridos tp : Atp) {
            System.out.println(tp);
        }
    }

    public ArrayList<Recorridos> cargarURLx() throws StreamReadException, DatabindException, MalformedURLException, IOException {
        String url_recorrido = "https://www.red.cl/restservice_v2/rest/getservicios/all";
        ArrayList<Recorridos> Aes = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode elementos_recorrido = mapper.readTree(new URL(url_recorrido));

        //ida
        for (JsonNode recorrido_x : elementos_recorrido) {
            String url = "https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint=" + recorrido_x.asText(); //API 
            JsonNode elementos = mapper.readTree(new URL(url));
            JsonNode negocio = elementos.get("negocio");
            JsonNode ida = elementos.get("ida");
            JsonNode regreso = elementos.get("regreso");

            if (ida == null) {
                Recorridos es = new Recorridos();
                es.setRecorrido(recorrido_x.asText());
                es.setId_empresa(negocio.get("id").asText());
                es.setNombre_empresa(negocio.get("nombre").asText());
                es.setId_ida("N/A");
                es.setTipo_dia("N/A");
                es.setInicio_dia("N/A");
                es.setFin_dia("N/A");
                es.setDestino("N/A");
                es.setParadero("N/A");
                es.setCod_paradero("N/A");
                es.setComuna_paradero("N/A");
                System.out.println(es);
                Aes.add(es);
            } else {
                JsonNode ida_horario = ida.get("horarios");
                if (ida_horario.isArray()) {
                    for (JsonNode horario : ida_horario) {
                        JsonNode paradero_ida = ida.get("paraderos");

                        for (JsonNode paradero : paradero_ida) {
                            Recorridos es = new Recorridos();
                            es.setRecorrido(recorrido_x.asText());
                            es.setId_empresa(negocio.get("id").asText());
                            es.setNombre_empresa(negocio.get("nombre").asText());
                            es.setId_ida(ida.get("id").asText());
                            es.setTipo_dia(horario.get("tipoDia").asText());
                            es.setInicio_dia(horario.get("inicio").asText());
                            es.setFin_dia(horario.get("fin").asText());
                            es.setDestino(ida.get("destino").asText());
                            es.setParadero(paradero.get("name").asText());
                            es.setCod_paradero(paradero.get("cod").asText());
                            es.setComuna_paradero(paradero.get("comuna").asText());
                            System.out.println(es);
                            Aes.add(es);
                        }

                    }
                }
            }
            if (regreso == null) {
                Recorridos es = new Recorridos();
                es.setRecorrido(recorrido_x.asText());
                es.setId_empresa(negocio.get("id").asText());
                es.setNombre_empresa(negocio.get("nombre").asText());
                es.setId_ida("N/A");
                es.setTipo_dia("N/A");
                es.setInicio_dia("N/A");
                es.setFin_dia("N/A");
                es.setDestino("N/A");
                es.setParadero("N/A");
                es.setCod_paradero("N/A");
                es.setComuna_paradero("N/A");
                System.out.println(es);
                Aes.add(es);
            } else {
                JsonNode regreso_horario = regreso.get("horarios");
                //dato vuelta
                if (regreso_horario.isArray()) {
                    for (JsonNode horario : regreso_horario) {
                        JsonNode paradero_regreso = regreso.get("paraderos");

                        for (JsonNode paradero : paradero_regreso) {
                            Recorridos es = new Recorridos();
                            es.setRecorrido(recorrido_x.asText());
                            es.setId_empresa(negocio.get("id").asText());
                            es.setNombre_empresa(negocio.get("nombre").asText());
                            es.setId_ida(regreso.get("id").asText());
                            es.setTipo_dia(horario.get("tipoDia").asText());
                            es.setInicio_dia(horario.get("inicio").asText());
                            es.setFin_dia(horario.get("fin").asText());
                            es.setDestino(regreso.get("destino").asText());
                            es.setParadero(paradero.get("name").asText());
                            es.setCod_paradero(paradero.get("cod").asText());
                            es.setComuna_paradero(paradero.get("comuna").asText());
                            System.out.println(es);
                            Aes.add(es);
                        }

                    }
                }

            }
        }

        return Aes;
    }
}
