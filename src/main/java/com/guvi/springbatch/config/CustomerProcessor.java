package com.guvi.springbatch.config;

import com.guvi.springbatch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {
    @Override
    public Customer process(Customer customer) throws Exception {
        //business filter,sorting ,chunk
        //condition
        if(customer.getCountry().equals("China")){
            return  customer;
        }else{
            return  null;
        }
//        return customer;
    }
}
