package com.ychenchen.state;

/**
 * @author alexis.yang
 * @since 2021/2/23 9:25 AM
 */
public class A12OrderInfo2 {
    //订单ID
    private Long orderId;
    //下单时间
    private String orderDate;
    //下单地址
    private String address;

    public A12OrderInfo2() {
    }

    public A12OrderInfo2(Long orderId, String orderDate, String address) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.address = address;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "A12OrderInfo2{" +
                "orderId=" + orderId +
                ", orderDate='" + orderDate + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

    public static A12OrderInfo2 string2OrderInfo2(String line) {
        A12OrderInfo2 a12OrderInfo2 = new A12OrderInfo2();
        if (line != null && line.length() > 0) {
            String[] fields = line.split(",");
            a12OrderInfo2.setOrderId(Long.parseLong(fields[0]));
            a12OrderInfo2.setOrderDate(fields[1]);
            a12OrderInfo2.setAddress(fields[2]);
        }
        return a12OrderInfo2;
    }
}
