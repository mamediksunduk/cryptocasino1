import aiohttp
from decimal import Decimal
from typing import Optional, Dict, List, Union
import logging

class CryptoPayAPI:
    def __init__(self, api_token: str, testnet: bool = False):
        self.api_token = api_token
        self.base_url = "https://testnet-pay.crypt.bot/api" if testnet else "https://pay.crypt.bot/api"
        self.headers = {"Crypto-Pay-API-Token": api_token}

    async def delete_check(self, check_id: int) -> Dict:
        return await self._make_request("POST", "deleteCheck", json={"check_id": check_id})

    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict:
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                f"{self.base_url}/{endpoint}",
                headers=self.headers,
                **kwargs
            ) as response:
                try:
                    return await response.json()
                except Exception as e:
                    logging.error(f"Failed to parse JSON response: {e}")
                    return {}

    async def create_invoice(
        self,
        amount: Union[str, float, Decimal],
        asset: str = "USDT",
        description: Optional[str] = None,
        hidden_message: Optional[str] = None,
        paid_btn_name: Optional[str] = None,
        paid_btn_url: Optional[str] = None,
        payload: Optional[str] = None,
        allow_comments: bool = True,
        allow_anonymous: bool = True,
        expires_in: Optional[int] = None
    ) -> Dict:
        data = {
            "asset": asset,
            "amount": str(amount),
            "description": description,
            "hidden_message": hidden_message,
            "paid_btn_name": paid_btn_name,
            "paid_btn_url": paid_btn_url,
            "payload": payload,
            "allow_comments": allow_comments,
            "allow_anonymous": allow_anonymous,
            "expires_in": expires_in
        }
        data = {k: v for k, v in data.items() if v is not None}
        return await self._make_request("POST", "createInvoice", json=data)

    async def transfer(
        self,
        user_id: int,
        asset: str,
        amount: str,
        spend_id: str,
        comment: Optional[str] = None,
        disable_send_notification: bool = False
    ) -> Dict:
        data = {
            "user_id": user_id,
            "asset": asset,
            "amount": amount,
            "spend_id": spend_id,
            "comment": comment,
            "disable_send_notification": disable_send_notification
        }
        data = {k: v for k, v in data.items() if v is not None}
        return await self._make_request("POST", "transfer", json=data)

    async def create_check(
        self,
        amount: str,
        asset: str = "USDT",
        description: Optional[str] = None,
        hidden_message: Optional[str] = None,
        payload: Optional[str] = None,
        expires_in: Optional[int] = None
    ) -> Dict:
        data = {
            "asset": asset,
            "amount": amount,
            "description": description,
            "hidden_message": hidden_message,
            "payload": payload,
            "expires_in": expires_in
        }
        data = {k: v for k, v in data.items() if v is not None}
        return await self._make_request("POST", "createCheck", json=data)

    async def get_balance(self) -> Dict:
        return await self._make_request("GET", "getBalance")

    async def get_exchange_rates(self) -> List[Dict]:
        response = await self._make_request("GET", "getExchangeRates")
        return response.get("result", [])

    async def get_checks(self, status: Optional[str] = None, asset: Optional[str] = None) -> Dict:
        params = {k: v for k, v in (("status", status), ("asset", asset)) if v is not None}
        return await self._make_request("GET", "getChecks", params=params)

    async def get_invoices(
        self,
        status: Optional[str] = None,
        asset: Optional[str] = None,
        offset: int = 0,
        count: int = 100
    ) -> Dict:
        params = {"offset": offset, "count": count}
        if status is not None:
            params["status"] = status
        if asset is not None:
            params["asset"] = asset
        return await self._make_request("GET", "getInvoices", params=params)