package com.ychenchen.state;

/**
 * @author alexis.yang
 * @since 2021/2/23 9:21 AM
 */
public class A11OrderInfo1 {
    //订单ID
    private Long orderId;
    //商品名称
    private String productName;
    //价格
    private Double price;

    public A11OrderInfo1() {
    }

    public A11OrderInfo1(Long orderId, String productName, Double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.price = price;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "A11OrderInfo1{" +
                "orderId=" + orderId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }

    public static A11OrderInfo1 string2OrderInfo1(String line) {
        A11OrderInfo1 a11OrderInfo1 = new A11OrderInfo1();
        if (line != null && line.length() > 0) {
            String[] fields = line.split(",");
            a11OrderInfo1.setOrderId(Long.parseLong(fields[0]));
            a11OrderInfo1.setProductName(fields[1]);
            a11OrderInfo1.setPrice(Double.parseDouble(fields[2]));
        }
        return a11OrderInfo1;
    }
}
