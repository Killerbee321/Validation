package com.company;

public class Hamburger {
    private String name;
    private String rollType;
    private String meat;
    private boolean lettuce;
    private int lettuceCount;
    private boolean tomato;
    private int tomatoCount;
    private boolean carrot;
    private int carrotCount;
    private boolean otherAddon;
    private int otherAddonCount;
    private int basePrice;
    private int tomatoPrice;
    private int carrotPrice;
    private int lettucePrice;
    private int otherAddonPrice;

    public Hamburger(String name, String rollType, String meat, int price) {
        this.name = name;
        this.rollType = rollType;
        this.meat = meat;
        this.basePrice = price;
    }
    public void finalPrice(){

        System.out.println("Final Price is"+basePrice+finalTomatoPrice()+finalCarrotPrice()+finalLettucePrice()+finalOtherAddonPrice());

    }




    public boolean isOtherAddon() {
        return otherAddon;
    }

    private int finalTomatoPrice(){
        if(tomato){
            return tomatoCount*tomatoPrice;
        }else{
            return 0;
        }
    }
    private int finalLettucePrice(){
        return lettuceCount*lettucePrice;
    }

    private int finalCarrotPrice(){
        return carrotCount*carrotPrice;
    }
    private int finalOtherAddonPrice(){
        return otherAddonCount*otherAddonPrice;
    }




















    public void setOtherAddon(boolean otherAddon) {
        this.otherAddon = otherAddon;
    }

    public int getOtherAddonCount() {
        return otherAddonCount;
    }

    public void setOtherAddonCount(int otherAddonCount) {
        this.otherAddonCount = otherAddonCount;
    }

    public boolean isLettuce() {
        return lettuce;
    }

    public void setLettuce(boolean lettuce) {
        this.lettuce = lettuce;
    }

    public int getLettuceCount() {
        return lettuceCount;
    }

    public void setLettuceCount(int lettuceCount) {
        this.lettuceCount = lettuceCount;
    }

    public boolean isTomato() {
        return tomato;
    }

    public void setTomato(boolean tomato) {
        this.tomato = tomato;
    }

    public int getTomatoCount() {
        return tomatoCount;
    }

    public void setTomatoCount(int tomatoCount) {
        this.tomatoCount = tomatoCount;
    }

    public boolean isCarrot() {
        return carrot;
    }

    public void setCarrot(boolean carrot) {
        this.carrot = carrot;
    }

    public int getCarrotCount() {
        return carrotCount;
    }

    public void setCarrotCount(int carrotCount) {
        this.carrotCount = carrotCount;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setRollType(String rollType) {
        this.rollType = rollType;
    }

    public void setMeat(String meat) {
        this.meat = meat;
    }

    public void setPrice(int price) {
        this.basePrice = price;
    }

    public String getName() {
        return name;
    }

    public String getRollType() {
        return rollType;
    }

    public String getMeat() {
        return meat;
    }

    public int getBasePrice() {
        return basePrice;
    }

    public int getTomatoPrice() {
        return tomatoPrice;
    }

    public int getCarrotPrice() {
        return carrotPrice;
    }

    public int getLettucePrice() {
        return lettucePrice;
    }

    public int getOtherAddonPrice() {
        return otherAddonPrice;
    }
}
