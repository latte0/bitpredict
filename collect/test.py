import pybitflyer

api = pybitflyer.API(api_key="...", api_secret="...")

tick = api.ticker(product_code="BTC_JPY")

print(tick)
