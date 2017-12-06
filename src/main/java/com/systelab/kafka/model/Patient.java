package com.systelab.kafka.model;

import javax.json.Json;
import javax.json.JsonObject;

public class Patient {
    Long id;
    String name;
    String surname;
    String country;

    public Patient(Long id) {
        this.id = id;
    }

    public Patient(Long id, String name, String surname, String country) {
        this.id = id;
        this.name = name;
        this.surname = surname;
        this.country = country;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String toJSON() {
        JsonObject json = Json.createObjectBuilder()
                .add("id", id)
                .add("name", name)
                .add("surname", surname)
                .add("country", country).build();
        return json.toString();
    }


}
