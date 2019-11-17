package com.company;

import java.util.ArrayList;

public class Arrays {
    private int[] array1=new int[10];
    private ArrayList<String> groceryList = new ArrayList<>();

    public void addGroceryItem(String item){
        groceryList.add(item);

    }

    public void printGroceryList(){
        System.out.println("grocerylist size is "+groceryList.size());
        for(int i=0;i<groceryList.size();i++){
            System.out.println(groceryList.get(i));
        }

    }

    public void modifyGroceryList(int pos,String newitem){
        groceryList.set(pos, newitem);
        System.out.println("item at"+pos+1+"modified");
    }

    public void removeGroceryItem(int pos){
        String theItem=groceryList.get(pos);
        groceryList.remove(pos);
    }



}
