import json
import pandas as pd

import db as db

table_name = ["booking_credentials", "flight_data", "hotel_data", "invoice_data"]


with open("data.json", "r") as f:
    datafile = json.loads(f.read())  # --------------Reading the Json File-------------

    # --------Single Document Handling--------------------
    for dataelement in datafile:
        # ---------------Initializing Tables--------------
        Column_Names = ["org_name", "expense_client_id", "Booking_ID", "BookingType"]
        dfMain = pd.DataFrame(columns=Column_Names)
        Column_Names = ["Booking_ID", "invoiceUrl", "No_Of_Invoices_with_Url"]
        dfInvoice = pd.DataFrame(columns=Column_Names)
        dfFlight = pd.DataFrame()
        dfHotel = pd.DataFrame()

        dflist = [dfMain, dfFlight, dfHotel, dfInvoice]

        # Appending to booking_credentials table
        datalist = []
        message = {
            "org_name": dataelement["org_name"],
            "expense_client_id": dataelement["expense_client_id"],
            "Booking_ID": dataelement["bookingId"],
            "BookingType": dataelement["booking_type"],
        }
        datalist.append(message)
        df2 = pd.DataFrame(datalist)
        dflist[0] = pd.concat([dflist[0], df2], ignore_index=True)

        db.upload_df_to_sql(dflist[0], table_name[0])
        # End of booking_credentials Table

        # Values that need to get reset for every file-----------

        original, adjusted = [], []
        keyslist = [original, adjusted]

        # ------------Flight Data Handling------------
        if dataelement["booking_type"] == "FLIGHT":
            flightlist = []
            bookingObj = dataelement["booking_data"]

            message = pd.Series(bookingObj[0])
            templist = [message]
            dfFlight = pd.DataFrame(
                templist
            )  # Creating a dataframe for a row entry for one record

            # --------Managing Column Names----------
            keyslist[0] = list(message.keys())
            keyslist[1] = [s.replace(" ", "_") for s in keyslist[0]]
            dfFlight.columns = keyslist[1]

        # ------------Hotel Data Handling-----------
        elif dataelement["booking_type"] == "HOTEL":
            hotelist = []
            bookingObj = dataelement["booking_data"]

            message = pd.Series(bookingObj[0])
            templist = [message]
            dfHotel = pd.DataFrame(
                templist
            )  # Creating a dataframe for a row entry for one record

            # --------Managing Column Names----------
            keyslist[0] = list(message.keys())
            keyslist[1] = [s.replace(" ", "_") for s in keyslist[0]]
            dfHotel.columns = keyslist[1]

        # -------Invoice Data Handling----------
        for InvoiceObj in dataelement["invoice_data"]:
            invoicelist = []
            ev, mmt, gst = 0, 0, 0
            count = [ev, mmt, gst]
            bookingID = InvoiceObj["bookingId"]
            eVl, mmtl, gstl = [], [], []
            invoiceurllist = [eVl, mmtl, gstl]
            for eVObj in InvoiceObj["invoiceTypeWiseData"]["eVOUCHER"]:
                try:
                    # print(f'Invoice Date = {GSTObj['invoiceDate']}, InvoiceURL = {GSTObj['invoiceUrl']}')
                    invoiceurllist[0].append(eVObj["invoiceUrl"])
                    count[0] += 1
                except KeyError:
                    # print(f'No eVOUCHER Invoice URL found')
                    message = "No Invoice Found"
                    invoiceurllist[0].append(message)
            for MMTObj in InvoiceObj["invoiceTypeWiseData"]["MMT"]:
                try:
                    # print(f'Invoice Date = {GSTObj['invoiceDate']}, InvoiceURL = {GSTObj['invoiceUrl']}')
                    invoiceurllist[1].append(MMTObj["invoiceUrl"])
                    count[1] += 1
                except KeyError:
                    # print(f'No MMT Invoice URL found')
                    message = "No Invoice Found"
                    invoiceurllist[1].append(message)
            for GSTObj in InvoiceObj["invoiceTypeWiseData"]["GST"]:
                try:
                    # print(f'Invoice Date = {GSTObj['invoiceDate']}, InvoiceURL = {GSTObj['invoiceUrl']}')
                    invoiceurllist[2].append(GSTObj["invoiceUrl"])
                    count[2] += 1
                except KeyError:
                    # print(f'No GST Invoice URL found')
                    message = "No Invoice Found"
                    invoiceurllist[2].append(message)
            invoiceurllist[0] = json.dumps(invoiceurllist[0])
            invoiceurllist[1] = json.dumps(invoiceurllist[1])
            invoiceurllist[2] = json.dumps(invoiceurllist[2])
            message = {
                "Booking_ID": bookingID,
                "eVoucherinvoiceUrl": invoiceurllist[1],
                "eVoucher_Invoices_with_Url": count[0],
                "MMTinvoiceUrl": invoiceurllist[1],
                "MMT_Invoices_with_Url": count[1],
                "GSTinvoiceUrl": invoiceurllist[2],
                "GST_Invoices_with_Url": count[2],
            }
            invoicelist.append(message)
            dftemp = pd.DataFrame(invoicelist)
            dflist[3] = pd.concat(
                [dflist[3], dftemp], ignore_index=True
            )  # Creating a row for the Invoice Table

        db.upload_df_to_sql(dflist[3], table_name[3])
        # ------Invoice Table Row Created-------
