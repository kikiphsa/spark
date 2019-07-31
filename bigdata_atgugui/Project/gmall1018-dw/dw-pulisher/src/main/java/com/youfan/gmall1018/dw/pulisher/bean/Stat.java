package com.youfan.gmall1018.dw.pulisher.bean;

import java.util.List;

/**
 * Create by chenqinping on 2019/5/9 20:23
 */
public class Stat {

    private String title;
    private List<Option> Options;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return Options;
    }

    public void setOptions(List<Option> options) {
        Options = options;
    }
}
