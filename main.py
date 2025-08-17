import asyncio
import aiohttp
from bs4 import BeautifulSoup as bs
from database import Database

class ShtylParser:
    def __init__(self, base_url='https://www.shtyl.ru'):
        self.headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        }

        self.base_url = base_url


    async def _get_soup(self, session: aiohttp.ClientSession, url: str) -> bs:
        """Получение обьекта bs """
        async with session.get(url ,headers=self.headers) as responce:
            responce.raise_for_status()
            text = await responce.text()
            return bs(text, 'lxml')


    async def _category(self, session: aiohttp.ClientSession) -> list:
        """ Собирает все ссылки на категории """
        soup = await self._get_soup(session, self.base_url)
        links = []

        for a in soup.select('a.stretched-link'):
            href = a.get('href')
            if href and href.startswith('/catalog/'):
                links.append(self.base_url + href)
        
        return links


    async def _get_product(self, session: aiohttp.ClientSession, url: str, products=None) -> list:
        if products is None:
            products = []

        soup = await self._get_soup(session, url)
        
        for item in soup.select("div.product-item-container"):
            name = item.select_one('[itemprop="name"]')
            desc = item.select_one('[itemprop="description"]')
            brand = item.select_one('[itemprop="brand"]')
            category = item.select_one('[itemprop="category"]')
            price = item.select_one('[itemprop="price"]')
            currency = item.select_one('[itemprop="priceCurrency"]')

            products.append({
                "id": item.get("data-product-id"),
                "name": name["content"] if name and name.has_attr("content") else None,
                "description": desc["content"] if desc and desc.has_attr("content") else None,
                "brand": brand["content"] if brand and brand.has_attr("content") else None,
                "category": category["content"] if category and category.has_attr("content") else None,
                "price": price["content"] if price and price.has_attr("content") else None,
                "currency": currency["content"] if currency and currency.has_attr("content") else None,
            })

        next_page = soup.select_one('[title="Вперед"]')
        if next_page:
            next_url = self.base_url + next_page['href']
            return await self._get_product(session, next_url, products)
            
        return products


    async def get_all_products(self) -> list:
        async with aiohttp.ClientSession() as session:
            categories = await self._category(session)
            print(f"Найдено {len(categories)} категорий.")

            tasks = [self._get_product(session, link) for link in categories]
            res = await asyncio.gather(*tasks)

            all_products = [item for sublist in res for item in sublist]
            return all_products


async def main():
    parser = ShtylParser()
    db = Database()

    all_products = await parser.get_all_products()
    print(f'Всего {len(all_products)} товаров.')

    db.insert_many(all_products)
    print("✅ Данные сохранены в базу.")

    # Проверка — достаём обратно
    products = db.fetch_all()
    print(f"В базе {len(products)} записей.")

    db.close()

if __name__ ==  '__main__':
    asyncio.run(main())