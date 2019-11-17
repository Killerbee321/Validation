package com.company;

public class Deluxe_Hamburger extends Hamburger{
    private int chips;
    private int drinks;
    private int chipsPrice;
    private int drinksPrice;

    public Deluxe_Hamburger(String name, String rollType, String meat, int price, int chips, int drinks, int chipsPrice, int drinksPrice) {
        super("Deluxe Hamburger", rollType, meat, price);
        this.chips = chips;
        this.drinks = drinks;
        this.chipsPrice = chipsPrice;
        this.drinksPrice = drinksPrice;
    }





    public int getChips() {
        return chips;
    }

    public int getDrinks() {
        return drinks;
    }

    public int getChipsPrice() {
        return chipsPrice;
    }

    public int getDrinksPrice() {
        return drinksPrice;
    }
}
