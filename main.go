package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/logicmonitor/lm-telemetry-sdk-go/config"
	"github.com/logicmonitor/lm-telemetry-sdk-go/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"	
)

var (
	tracer trace.Tracer
)

func initTracing() {
	ctx := context.Background()

	customAttributes := map[string]string{
		"service.namespace": "sample-namespace",
		"service.name":      "sample-service",
	}

	err := telemetry.SetupTelemetry(ctx,
		config.WithAttributes(customAttributes),
		config.WithAWSEC2Detector(),
		config.WithHTTPTraceEndpoint("20.83.118.211:4318"),
		config.WithSimlpeSpanProcessor(),
		//config.WithDefaultInAppExporter(),
	)
	if err != nil {
		log.Fatalf("error in setting up telemetry: %s", err.Error())
	}
	tracer = otel.Tracer("tracer-1")

}

func main() {
	initTracing()
	otelSalesOrdersHandler := otelhttp.NewHandler(http.HandlerFunc(salesOrdersHandler), "sales_orders")
	// http.HandleFunc("/sales_orders", salesOrdersHandler)
	http.Handle("/sales_orders", otelSalesOrdersHandler)
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
	time.Sleep(2 * time.Second)

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

