package org.apache.beam.examples;

public class ExpresoSantiago {

   private String recorrido;
    private String id_empresa;
    private String nombre_empresa;
    private String id_ida;
    private String tipo_dia;
    private String inicio_dia;
    private String fin_dia;
    private String paradero;
    private String cod_paradero;
    private String comuna_paradero;
    private String destino;

    public ExpresoSantiago() {
    }

    public String getId_empresa() {
        return id_empresa;
    }

    public String getNombre_empresa() {
        return nombre_empresa;
    }

    public String getId_ida() {
        return id_ida;
    }

    public void setId_empresa(String id_empresa) {
        this.id_empresa = id_empresa;
    }

    public void setNombre_empresa(String nombre_empresa) {
        this.nombre_empresa = nombre_empresa;
    }

    public void setId_ida(String id_ida) {
        this.id_ida = id_ida;
    }

    public String getTipo_dia() {
        return tipo_dia;
    }

    public void setTipo_dia(String tipo_dia) {
        this.tipo_dia = tipo_dia;
    }

    public String getInicio_dia() {
        return inicio_dia;
    }

    public String getFin_dia() {
        return fin_dia;
    }

    public void setInicio_dia(String inicio_dia) {
        this.inicio_dia = inicio_dia;
    }

    public void setFin_dia(String fin_dia) {
        this.fin_dia = fin_dia;
    }

    public String getParadero() {
        return paradero;
    }

    public void setParadero(String paradero) {
        this.paradero = paradero;
    }

    public String getDestino() {
        return destino;
    }

    public void setDestino(String destino) {
        this.destino = destino;
    }

    public String getCod_paradero() {
        return cod_paradero;
    }

    public void setCod_paradero(String cod_paradero) {
        this.cod_paradero = cod_paradero;
    }

    public String getComuna_paradero() {
        return comuna_paradero;
    }

    public void setComuna_paradero(String comuna_paradero) {
        this.comuna_paradero = comuna_paradero;
    }

    public String getRecorrido() {
        return recorrido;
    }

    public void setRecorrido(String recorrido) {
        this.recorrido = recorrido;
    }
    

    @Override
    public String toString() {
        return "ExpresoSantiago{" + "recorrido=" + recorrido + ", id_empresa=" + id_empresa + ", nombre_empresa=" + nombre_empresa + ", id_ida=" + id_ida + ", tipo_dia=" + tipo_dia + ", inicio_dia=" + inicio_dia + ", fin_dia=" + fin_dia + ", paradero=" + paradero + ", cod_paradero=" + cod_paradero + ", comuna_paradero=" + comuna_paradero + ", destino=" + destino + '}';
    }

  

    public String toCSV() {
        return ""+recorrido+","+id_empresa+","+nombre_empresa+","+id_ida+","+tipo_dia+","+inicio_dia+","+fin_dia+","+paradero+","+cod_paradero+","+comuna_paradero+","+destino+"";
            
    }
}
