package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"encoding/json"
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/sales_orders", salesOrdersHandler)
	http.ListenAndServe(":9090", nil)
}

func salesOrdersHandler(w http.ResponseWriter, req *http.Request) {

	params := map[string]interface{}{}
	err := json.NewDecoder(req.Body).Decode(&params)

	if err != nil {
		fmt.Fprintf(w, err.Error())
		fmt.Fprintf(w, "Failed to decode Json body.")

	} else {
		response, _ := createSalesOrder(params)

		fmt.Fprintf(w, response+"\r\n")
	}

}

func createSalesOrder(params map[string]interface{}) (string, error) {

	db, err := sql.Open("mysql", "store_db_user:Mysql!1221@tcp(127.0.0.1:3306)/store_db")
	if err != nil {
		return "", err
	}

	defer db.Close()

	tx, err := db.Begin()

	queryString := "insert into sales_orders(customer_id, order_date) values (?, ?)"

	response, err := tx.Exec(queryString, params["customer_id"], params["order_date"])

	if err != nil {
		tx.Rollback()
		return "Failed to create a sales order, transaction rolled back. Reason: " + err.Error() + "\r\n", err
	}

	orderId, err := response.LastInsertId()

	if err != nil {
		tx.Rollback()
		return "Failed to retrieve order_id, transaction rolled back. Reason: " + err.Error() + "\r\n", err
	}

	queryString = "insert into sales_products(order_id, product_id, qty) values (?, ?, ?)"

	product := map[string]interface{}{}

	for _, value := range params["sales_products"].([]interface{}) {

		product = value.(map[string]interface{})

		_, err := tx.Exec(queryString, orderId, product["product_id"], product["qty"])

		if err != nil {
			tx.Rollback()
			return "Failed to insert sales order product. Transaction rolled back. Reason: " + err.Error() + "\r\n", err
		}

	}

	err = tx.Commit()

	if err != nil {
		return "Failed to create the order.\r\n,", err
	}

	return "Success, Sales id is: " + fmt.Sprint(orderId) + "\r\n", nil
}

