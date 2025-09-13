import pandas as pd
import json
import urllib
from datetime import datetime

# Load the order CSV file
df = pd.read_csv("/content/Orders (1).csv")

# List of test order IDs to exclude
test_order_ids = [10001, 10002, 10003, 10004, 10049, 10061, 10114, 10115, 10450, 10451, 10452]

# Output list
expanded_rows = []

count = 0

for _, row in df.iterrows():
    count = count + 1
    try:
        line_items = json.loads(row.get('Line Items', '[]'))
        shipping_info = json.loads(row.get('Shipping Info', '{}'))
        activities = json.loads(row.get('Activities', '[]'))
        totals = json.loads(row.get('Totals', '{}'))
        fulfillments = json.loads(row.get("Fulfillments", "[]"))
    except Exception as e:
        continue

    order_id = int(row.get("Number"))
    if order_id in test_order_ids:
        continue

    order_date = next((a.get("timestamp") for a in activities if a.get("type") == "ORDER_PLACED"), None)
    shipment = shipping_info.get("shipmentDetails") or {}
    address = shipment.get("address", {})

    fulfillment_status = row.get("Fulfillment Status", "").strip().upper()
    tracking_info = fulfillments[0].get("trackingInfo", {}) if fulfillments else {}

    shared_info = {
        "Order ID": order_id,
        "Order Date": order_date,
        "Payment Status": row.get("Payment Status", "").strip().upper(),
        "Fulfillment Status": fulfillment_status,
        "Tracking Number": tracking_info.get("trackingNumber", ""),
        "Shipping Provider": tracking_info.get("shippingProvider", ""),
        "First Name": shipment.get("firstName", "").strip().title(),
        "Last Name": shipment.get("lastName", ""),
        "Email": shipment.get("email", ""),
        "Phone": shipment.get("phone", ""),
        "Delivery Option": shipping_info.get("deliveryOption", ""),
        "Estimated Delivery": shipping_info.get("estimatedDeliveryTime", ""),
        "City": address.get("city", ""),
        "Street Address": address.get("addressLine", ""),
        "Country": address.get("country", ""),
        "Postal Code": address.get("postalCode", ""),
        "Weight": totals.get("weight", ""),
        "Subtotal": totals.get("subtotal", ""),
        "Tax": totals.get("tax", ""),
        "Shipping Charge": totals.get("shipping", ""),
        "Discount": totals.get("discount", ""),
        "Total Amount": totals.get("total", "")
    }

    for item in line_items:
        options = item.get("options", [])
        option_selections = {opt["option"]: opt["selection"] for opt in options}
        custom_texts = item.get("customTextFields", [])
        custom_text_data = {
            text_field.get("title", ""): text_field.get("value", "")
            for text_field in custom_texts
        }

        item_row = {
            "Translated Name": item.get("translatedName"),
            "SKU": item.get("sku"),
            "Quantity": item.get("quantity"),
            "Total Price": item.get("totalPrice"),
            "Size": option_selections.get("Sizes", ""),
            "Color": option_selections.get("Colour", ""),
            "Custom Size Note": custom_text_data.get("Custom Size (if selected)", "")
        }

        expanded_rows.append({**shared_info, **item_row})

# Create final DataFrame
final_df = pd.DataFrame(expanded_rows)

# --- Make Order ID Unique Per Item ---
final_df["Original Order ID"] = final_df["Order ID"]
final_df["Item Index"] = final_df.groupby("Order ID").cumcount() + 1

# Example: assume you already have final_df with "Order ID" and "Item Index"
# Add date and weekday
today_str = datetime.today().strftime("%Y%m%d")        # e.g. 20250906
weekday_str = datetime.today().strftime("%a").upper()        # e.g. SAT

# Build new Order ID
final_df["Order ID"] = (
    final_df["Order ID"].astype(str)
    + "Q" + final_df["Item Index"].astype(str)
    + today_str
    + weekday_str
)

# Save main parsed order data
final_df.to_csv("orders_processed_02.csv", index=False)

# --- Delhivery Manifestable File Generator ---
manifest_df = pd.DataFrame()
final_df["Sale Order Number"] = "PZ"+final_df["Order ID"]
final_df["Pickup Location Name"] = "Preetizen Lifestyle"
final_df["Transport Mode"] = "Surface"
final_df["Payment Mode"] = final_df["Payment Status"].apply(lambda x: "Prepaid" if x == "PAID" else "COD")
final_df["Customer Name"] = final_df["First Name"] + " " + final_df["Last Name"]
final_df["Customer Phone"] = final_df["Phone"]
final_df["Shipping Address Line1"] = final_df["Street Address"]
final_df["Shipping City"] = final_df["City"]
final_df["Shipping Pincode"] = final_df["Postal Code"]
# pin["pincode"] = pin["pincode"].astype('O')
# final_df = pd.merge(final_df, pin[['pincode', 'state']], left_on= 'Shipping Pincode', right_on = 'pincode', how = 'left')
final_df["Shipping State"] = 'West Bengal'

final_df["Item Sku Code"] = final_df["SKU"]
final_df["Item Sku Name"] = final_df["Translated Name"] + " - Size: " + final_df["Size"].str.upper() + " - Colour: " + final_df["Color"]
final_df["Quantity Ordered"] = final_df["Quantity"]

# Compute item price after discount
def calculate_unit_price(row):
    base_price = row["Total Price"] - row["Discount"]
    shipping = 80 if row["Payment Status"] != "PAID" and base_price < 2000 else 0
    return base_price + shipping

final_df["Unit Item Price"] = final_df.apply(calculate_unit_price, axis=1)
final_df["Length (cm)"] = 35
final_df["Breadth (cm)"] = 25
final_df["Height (cm)"] = 5
final_df["Weight (gm)"] = 250

exclude_cols = ["Length (cm)", "Breadth (cm)", "Height (cm)", "Weight (gm)"]

final_df.columns = [
    col if col in exclude_cols else f"*{col}"
    for col in final_df.columns
]

# Save Delhivery manifest
final_df.to_csv("delhivery_manifest_.csv", index=False)

final_df