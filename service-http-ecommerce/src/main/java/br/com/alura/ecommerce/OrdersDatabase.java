package br.com.alura.ecommerce;

import java.sql.SQLException;

public class OrdersDatabase {
    
    private final LocalDatabase database;
    
    OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        this.database.createIfNotExists("""
                    create table Orders 
                    (uuid varchar(200) primary key,
                    email varchar(200),
                    is_fraud boolean)
                    """);
    }

    public boolean saveNew(Order order) throws SQLException {
        if(wasProcessed(order)){
            return false;
        }
        database.update("""
                    insert into Orders (uuid,is_fraud) values (?, true)
                    """, order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    public void close() throws SQLException {
        database.close();
    }
}
