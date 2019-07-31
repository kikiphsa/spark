package com.youfan.gmall1018.dw.pulisher.bean;

/**
 * Create by chenqinping on 2019/5/9 20:23
 */
public class Option {

    private String name;

    private Double value;

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
