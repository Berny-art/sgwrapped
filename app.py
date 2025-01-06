from flask import Flask, jsonify
import aiohttp
import asyncio

app = Flask(__name__)

# Base URLs for queries
BASE_URLS = {
    "new_user": "https://metabase.constellations.zone/api/public/card/0768024b-0379-41d1-8e99-c3b652e4a208/query/json",
    "mints_data": "https://metabase.constellations.zone/api/public/card/8690f6a5-f60b-4491-80aa-ee469d80edcc/query/json",
    "nfts_burned": "https://metabase.constellations.zone/api/public/card/888a3217-1af1-40c5-813f-fef9324def24/query/json",
    "rarest_mint": "https://metabase.constellations.zone/api/public/card/b195a259-2386-4688-8c99-63d114a75bcf/query/json",
    "most_active_day": "https://metabase.constellations.zone/api/public/card/a3893024-6427-4b0b-b2a7-481ac5ac55ce/query/json",
    "most_active_month": "https://metabase.constellations.zone/api/public/card/ce538b2b-4c55-44fa-8d1c-3b65c8594b6f/query/json",
    "marketplace_volume": "https://metabase.constellations.zone/api/public/card/f934f4a4-79a6-4447-ac7e-30b2933d1d9f/query/json",
    "best_trade": "https://metabase.constellations.zone/api/public/card/48494a37-a828-405e-8247-2e60c4f5d0da/query/json",
    "quickest_flip": "https://metabase.constellations.zone/api/public/card/0aa67d7a-cf7c-40c9-aa25-1ae925555a9f/query/json",
    "most_active_collections": "https://metabase.constellations.zone/api/public/card/8ffd5859-dc96-4ed6-8ae0-a5b889ccf439/query/json",
}

async def fetch_data(session, url, address):
    """Helper function to fetch data from a single Metabase endpoint."""
    params = {
        "parameters": f'[{{"type":"category","value":"{address}","id":"7bd8f6e0-9d42-4779-ab5d-09af2683b293","target":["variable",["template-tag","address"]]}}]'
    }
    async with session.get(url, params=params) as response:
        return await response.json()

async def fetch_all(address):
    """Fetch all data asynchronously from Metabase."""
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_data(session, url, address) for url in BASE_URLS.values()
        ]
        return await asyncio.gather(*tasks)

@app.route("/api/wrapped/2024/<address>", methods=["GET"])
async def aggregate_data(address):
    """Aggregate data from multiple Metabase endpoints."""
    try:
        results = await fetch_all(address)
        response_data = dict(zip(BASE_URLS.keys(), results))
        return jsonify(response_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ != "__main__":
    # Import the app for Render deployment
    from app import app
