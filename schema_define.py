# Databricks notebook source
data_json = spark.read.json("dbfs:/FileStore/vtex_test_schema/test_schema.json",multiLine=True)

# COMMAND ----------

data_json.printSchema()

# COMMAND ----------

from pyspark.sql.functions import udf
import re
from pyspark.sql import DataFrame

# COMMAND ----------

from pyspark.sql.types import *
clientPreferencesDataSchema = StructType([
    StructField("locale", StringType(), True),
    StructField("optinNewsLetter", BooleanType(), True)
])

clientProfileDataSchema = StructType([
    StructField("corporateDocument", StringType(), True),
    StructField("corporateName", StringType(), True),
    StructField("corporatePhone", StringType(), True),
    StructField("customerClass", StringType(), True),
    StructField("document", StringType(), True),
    StructField("documentType", StringType(), True),
    StructField("email", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("id", StringType(), True),
    StructField("isCorporate", BooleanType(), True),
    StructField("lastName", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("stateInscription", StringType(), True),
    StructField("tradeName", StringType(), True),
    StructField("userProfileId", StringType(), True),
    StructField("userProfileVersion", StringType(), True)
])

itemMetadataSchema = StructType([
    StructField("Items", ArrayType(
        StructType([
            StructField("AssemblyOptions", ArrayType(
                StructType([
                    StructField("Composition", StringType(), True),
                    StructField("Id", StringType(), True),
                    StructField("InputValues", StructType([
                        StructField("garantia-estendida", StructType([
                            StructField("Domain", ArrayType(StringType(), True), True),
                            StructField("MaximumNumberOfCharacters", LongType(), True)
                        ]), True),
                        StructField("takeback", StructType([
                            StructField("Domain", ArrayType(StringType(), True), True),
                            StructField("MaximumNumberOfCharacters", LongType(), True)
                        ]), True)
                    ]), True),
                    StructField("Name", StringType(), True),
                    StructField("Required", BooleanType(), True)
                ]), True), True),
            StructField("DetailUrl", StringType(), True),
            StructField("Ean", StringType(), True),
            StructField("Id", StringType(), True),
            StructField("ImageUrl", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("ProductId", StringType(), True),
            StructField("RefId", StringType(), True),
            StructField("Seller", StringType(), True),
            StructField("SkuName", StringType(), True)
        ]), True), True)
])

addressSchema = StructType([
    StructField("addressId", StringType(), True),
    StructField("addressType", StringType(), True),
    StructField("city", StringType(), True),
    StructField("complement", StringType(), True),
    StructField("country", StringType(), True),
    StructField("entityId", StringType(), True),
    StructField("geoCoordinates", ArrayType(DoubleType(), True), True),
    StructField("neighborhood", StringType(), True),
    StructField("number", StringType(), True),
    StructField("postalCode", StringType(), True),
    StructField("receiverName", StringType(), True),
    StructField("reference", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True),
    StructField("versionId", StringType(), True)
])

pickupStoreInfoSchema = StructType([
    StructField("additionalInfo", StringType(), True),
    StructField("address", addressSchema, True),
    StructField("dockId", StringType(), True),
    StructField("friendlyName", StringType(), True),
    StructField("isPickupStore", BooleanType(), True)
])

deliveryIdsSchema = StructType([
    StructField("accountCarrierName", StringType(), True),
    StructField("courierId", StringType(), True),
    StructField("courierName", StringType(), True),
    StructField("dockId", StringType(), True),
    StructField("kitItemDetails", ArrayType(StringType(), True), True),
    StructField("quantity", LongType(), True),
    StructField("warehouseId", StringType(), True)
])

deliveryChannelsSchema = StructType([
    StructField("id", StringType(), True),
    StructField("stockBalance", LongType(), True)
])

slasSchema = StructType([
    StructField("deliveryChannel", StringType(), True),
    StructField("deliveryWindow", StringType(), True),
    StructField("id", StringType(), True),
    StructField("lockTTL", StringType(), True),
    StructField("name", StringType(), True),
    StructField("pickupDistance", DoubleType(), True),
    StructField("pickupPointId", StringType(), True),
    StructField("pickupStoreInfo", pickupStoreInfoSchema, True),
    StructField("polygonName", StringType(), True),
    StructField("price", LongType(), True),
    StructField("shippingEstimate", StringType(), True),
    StructField("transitTime", StringType(), True),
    StructField("versionId", StringType(), True)
])

logisticsInfoSchema = StructType([
    StructField("addressId", StringType(), True),
    StructField("deliveryChannel", StringType(), True),
    StructField("deliveryChannels", ArrayType(deliveryChannelsSchema, True), True),
    StructField("deliveryCompany", StringType(), True),
    StructField("deliveryIds", ArrayType(deliveryIdsSchema, True), True),
    StructField("deliveryWindow", StringType(), True),
    StructField("entityId", StringType(), True),
    StructField("itemIndex", LongType(), True),
    StructField("listPrice", LongType(), True),
    StructField("lockTTL", StringType(), True),
    StructField("pickupPointId", StringType(), True),
    StructField("pickupStoreInfo", pickupStoreInfoSchema, True),
    StructField("polygonName", StringType(), True),
    StructField("price", LongType(), True),
    StructField("selectedSla", StringType(), True),
    StructField("sellingPrice", LongType(), True),
    StructField("shippingEstimate", StringType(), True),
    StructField("shippingEstimateDate", StringType(), True),
    StructField("shipsTo", ArrayType(StringType(), True), True),
    StructField("slas", ArrayType(slasSchema, True), True),
    StructField("transitTime", StringType(), True),
    StructField("versionId", StringType(), True)
])

shippingDataSchema = StructType([
    StructField("address", addressSchema, True),
    StructField("id", StringType(), True),
    StructField("logisticsInfo", ArrayType(logisticsInfoSchema, True), True),
    StructField("selectedAddresses", ArrayType(addressSchema, True), True),
    StructField("trackingHints", StringType(), True)
])

currencyFormatInfoSchema = StructType([
    StructField("CurrencyDecimalDigits", LongType(), True),
    StructField("CurrencyDecimalSeparator", StringType(), True),
    StructField("CurrencyGroupSeparator", StringType(), True),
    StructField("CurrencyGroupSize", LongType(), True),
    StructField("StartsWithCurrencySymbol", BooleanType(), True)
])

storePreferencesDataSchema = StructType([
    StructField("countryCode", StringType(), True),
    StructField("currencyCode", StringType(), True),
    StructField("currencyFormatInfo", currencyFormatInfoSchema, True),
    StructField("currencyLocale", LongType(), True),
    StructField("currencySymbol", StringType(), True),
    StructField("timeZone", StringType(), True)
])

totalsSchema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("value", LongType(), True)
])

schema = StructType([
    StructField("affiliateId", StringType(), True),
    StructField("allowCancellation", BooleanType(), True),
    StructField("allowEdition", BooleanType(), True),
    StructField("authorizedDate", StringType(), True),
    StructField("callCenterOperatorData", StringType(), True),
    StructField("cancelReason", StringType(), True),
    StructField("cancellationData", StringType(), True),
    StructField("changesAttachment", StringType(), True),
    StructField("checkedInPickupPointId", StringType(), True),
    StructField("clientPreferencesData", clientPreferencesDataSchema, True),
    StructField("clientProfileData", clientProfileDataSchema, True),
    StructField("commercialConditionData", StringType(), True),
    StructField("creationDate", StringType(), True),
    StructField("customData", StringType(), True),
    StructField("followUpEmail", StringType(), True),
    StructField("giftRegistryData", StringType(), True),
    StructField("hostname", StringType(), True),
    StructField("invoiceData", StringType(), True),
    StructField("invoicedDate", StringType(), True),
    StructField("isCheckedIn", BooleanType(), True),
    StructField("isCompleted", BooleanType(), True),
    StructField("itemMetadata", itemMetadataSchema, True),
    StructField("items", ArrayType(
        StructType([
            StructField("additionalInfo", StringType(), True),
            StructField("attachments", ArrayType(
                StructType([
                    StructField("content", StringType(), True),
                    StructField("name", StringType(), True)
                ]), True), True),
            StructField("availability", StringType(), True),
            StructField("detailUrl", StringType(), True),
            StructField("id", StringType(), True),
            StructField("imageUrl", StringType(), True),
            StructField("listPrice", LongType(), True),
            StructField("measurementUnit", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", LongType(), True),
            StructField("quantity", LongType(), True),
            StructField("seller", StringType(), True),
            StructField("sellingPrice", LongType(), True),
            StructField("skuName", StringType(), True),
            StructField("uniqueId", StringType(), True),
            StructField("unitMultiplier", LongType(), True),
            StructField("url", StringType(), True)
        ]), True), True),
    StructField("lastChange", StringType(), True),
    StructField("lastMessage", StringType(), True),
    StructField("marketplace", StructType([
        StructField("baseURL", StringType(), True),
        StructField("isCertified", BooleanType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("marketplaceItems", ArrayType(StringType(), True), True),
    StructField("marketplaceOrderId", StringType(), True),
    StructField("marketplaceServicesEndpoint", StringType(), True),
    StructField("merchantName", StringType(), True),
    StructField("openTextField", StringType(), True),
    StructField("orderFormId", StringType(), True),
    StructField("orderGroup", StringType(), True),
    StructField("orderId", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("packageAttachment", StructType([
        StructField("packages", ArrayType(StringType(), True), True)
    ]), True),
    StructField("paymentData", StructType([
        StructField("giftCards", ArrayType(StringType(), True), True),
        StructField("transactions", ArrayType(StringType(), True), True)
    ]), True),
    StructField("ratesAndBenefitsData", StructType([
        StructField("id", StringType(), True),
        StructField("rateAndBenefitsIdentifiers", ArrayType(StringType(), True), True)
    ]), True),
    StructField("roundingError", LongType(), True),
    StructField("salesChannel", StringType(), True),
    StructField("sellerOrderId", StringType(), True),
    StructField("sellers", ArrayType(
        StructType([
            StructField("fulfillmentEndpoint", StringType(), True),
            StructField("id", StringType(), True),
            StructField("logo", StringType(), True),
            StructField("name", StringType(), True)
        ]), True), True),
    StructField("sequence", StringType(), True),
    StructField("shippingData", shippingDataSchema, True),
    StructField("status", StringType(), True),
    StructField("statusDescription", StringType(), True),
    StructField("storePreferencesData", storePreferencesDataSchema, True),
    StructField("subscriptionData", StringType(), True),
    StructField("taxData", StringType(), True),
    StructField("totals", ArrayType(totalsSchema, True), True),
    StructField("value", LongType(), True),
    StructField("workflowIsInError", BooleanType(), True)
])



# COMMAND ----------

# df = spark.createDataFrame(data_json,schema)
data_json.display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
df= spark.read.json(path="dbfs:/FileStore/vtex_test_schema/vtex_test_data__1_.json")
flatten_df=df.withColumn("orderDetails", from_json("orderDetails",schema))
flatten_df = flatten_df.select("orderDetails.*")
display(flatten_df)

# COMMAND ----------

df= spark.read.json(path="dbfs:/FileStore/vtex_test_schema/vtex_test_data__1_.json").display()

# COMMAND ----------

from pyspark.sql.functions import udf
import re
from pyspark.sql import DataFrame

# COMMAND ----------



# COMMAND ----------

def to_snake_case(df: DataFrame) -> DataFrame:
    def convert(name: str) -> str:
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
        return name.lower()

    for column in df.columns:
        snake_case_col = convert(column)
        df = df.withColumnRenamed(column, snake_case_col)
    
    return df
 
udf(to_snake_case)
 