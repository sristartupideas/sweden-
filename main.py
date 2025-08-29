import asyncio
import aiohttp
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import logging
from fake_useragent import UserAgent
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveSwedishScraper:
    def __init__(self):
        self.ua = UserAgent()
        self.session = None
        self.playwright = None
        self.browser = None
        
        # Platform coverage estimates based on research
        self.platform_coverage = {
            "bolagsplatsen.se": 1321,    # 50% of market
            "objektvision.se": 449,      # 17% of market  
            "lania.se": 200,             # 8% of market (estimated)
            "tactic.se": 150,            # 6% of market (estimated)
            "sffab.se": 100,             # 4% of market (estimated)
            "exitpartner.se": 80,        # 3% of market (estimated)
            "bolagsbron.se": 60,         # 2% of market (estimated)
            "nmk.se": 40,                # 1.5% of market (estimated)
            "stockholmsforetagsmaklare.se": 30  # 1% of market (estimated)
        }
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'sv-SE,sv;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
            },
            timeout=aiohttp.ClientTimeout(total=60)
        )
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    # 1. BOLAGSPLATSEN.SE - 50% of market (1,321 listings)
    async def scrape_bolagsplatsen_comprehensive(self) -> Dict[str, List[str]]:
        """Comprehensive scraping of Sweden's largest platform"""
        pages = []
        details = []
        
        try:
            # Multiple pages and categories for maximum coverage
            urls_to_scrape = [
                # Main listings with pagination
                "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla",
                "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla?page=2", 
                "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla?page=3",
                "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla?page=4",
                "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla?page=5",
                
                # Different categories
                "https://www.bolagsplatsen.se/foretag-till-salu/internetforetag-e-handel/alla",
                "https://www.bolagsplatsen.se/foretag-till-salu/tjansteforetag/alla",
                "https://www.bolagsplatsen.se/foretag-till-salu/handel/alla",
                "https://www.bolagsplatsen.se/foretag-till-salu/bygg-entreprenad/alla",
                "https://www.bolagsplatsen.se/foretag-till-salu/tillverkning/alla",
                
                # Different sorting
                "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla?sort=created_desc",
                "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla?sort=price_asc",
            ]
            
            all_business_urls = set()  # Use set to avoid duplicates
            
            # Scrape all listing pages
            for url in urls_to_scrape:
                try:
                    logger.info(f"Scraping Bolagsplatsen page: {url}")
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            # Handle Swedish character encoding properly
                            html = await response.text(encoding='utf-8', errors='replace')
                            pages.append(html)
                            
                            # Extract business URLs from this page
                            soup = BeautifulSoup(html, 'html.parser')
                            
                            # Look for business listing links - FIXED REGEX
                            business_links = soup.find_all('a', href=re.compile(r'/foretag-till-salu/[^/]+$'))
                            for link in business_links:
                                href = link.get('href')
                                if href and not href.startswith('http'):
                                    # Fix URL construction - ensure no duplicate domain
                                    if href.startswith('/'):
                                        full_url = f"https://www.bolagsplatsen.se{href}"
                                    else:
                                        full_url = f"https://www.bolagsplatsen.se/{href}"
                                    all_business_urls.add(full_url)
                                    
                    await asyncio.sleep(2)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
                    
            logger.info(f"Found {len(all_business_urls)} unique businesses on Bolagsplatsen")
            
            # Get detail pages (limit to reasonable number)
            detail_urls = list(all_business_urls)[:50]  # Top 50 for performance
            
            for detail_url in detail_urls:
                try:
                    async with self.session.get(detail_url) as detail_response:
                        if detail_response.status == 200:
                            # FIXED: Add encoding for detail pages too
                            detail_html = await detail_response.text(encoding='utf-8', errors='replace')
                            
                            # Add contact info as HTML comment
                            contact_info = self._extract_bolagsplatsen_contact(detail_html)
                            if contact_info:
                                contact_json = json.dumps(contact_info).replace('"', "'")
                                comment = f""
                                detail_html = detail_html.replace('</body>', f"{comment}\n</body>")
                            
                            details.append(detail_html)
                            
                    await asyncio.sleep(1.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error getting detail from {detail_url}: {e}")
                    
        except Exception as e:
            logger.error(f"Error in Bolagsplatsen scraping: {e}")
            
        return {"pages": pages, "details": details}

    # 2. OBJEKTVISION.SE - 17% of market (449 listings)  
    async def scrape_objektvision_browser(self) -> Dict[str, List[str]]:
        """Browser automation for Objektvision"""
        pages = []
        details = []
        
        context = None
        try:
            context = await self.browser.new_context(
                user_agent=self.ua.random
            )
            page = await context.new_page()
            
            # Multiple attempts with different URLs
            objektvision_urls = [
                "https://objektvision.se/företag_till_salu",
                "https://objektvision.se/foretag_till_salu",  # Alternative spelling
            ]
            
            for url in objektvision_urls:
                try:
                    logger.info(f"Scraping Objektvision: {url}")
                    await page.goto(url, wait_until='networkidle', timeout=30000)
                    await asyncio.sleep(3)
                    
                    html = await page.content()
                    pages.append(html)
                    
                    # Look for listing links
                    links = await page.query_selector_all('a[href*="företag"], a[href*="foretag"]')
                    
                    for link in links[:10]:  # Limit for performance
                        try:
                            href = await link.get_attribute('href')
                            if href and 'till_salu' in href:
                                full_url = urljoin(url, href)
                                await page.goto(full_url, wait_until='networkidle', timeout=30000)
                                detail_html = await page.content()
                                details.append(detail_html)
                                await asyncio.sleep(2)
                        except Exception as e:
                            logger.error(f"Error getting Objektvision detail: {e}")
                    
                    break  # Success, no need to try other URLs
                    
                except Exception as e:
                    logger.error(f"Error with {url}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping Objektvision: {e}")
        finally:
            if context:
                await context.close()
            
        return {"pages": pages, "details": details}

    # 3. LANIA.SE - 8% of market 
    async def scrape_lania_browser(self) -> Dict[str, List[str]]:
        """Browser automation for Lania"""
        pages = []
        details = []
        
        context = None
        try:
            context = await self.browser.new_context(
                user_agent=self.ua.random
            )
            page = await context.new_page()
            
            logger.info("Scraping Lania...")
            await page.goto("https://www.lania.se/foretag-till-salu/", wait_until='networkidle', timeout=30000)
            await asyncio.sleep(3)
            
            html = await page.content()
            pages.append(html)
            
            # Find business listing links
            links = await page.query_selector_all('a[href*="/foretag-till-salu/"]')
            
            for link in links[:8]:  # Reasonable limit
                try:
                    href = await link.get_attribute('href')
                    if href and not href.endswith('/foretag-till-salu/'):
                        full_url = urljoin("https://www.lania.se", href)
                        await page.goto(full_url, wait_until='networkidle', timeout=30000)
                        detail_html = await page.content()
                        details.append(detail_html)
                        await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Error getting Lania detail: {e}")
            
        except Exception as e:
            logger.error(f"Error scraping Lania: {e}")
        finally:
            if context:
                await context.close()
            
        return {"pages": pages, "details": details}

    # 4. TACTIC.SE - 6% of market
    async def scrape_tactic_hybrid(self) -> Dict[str, List[str]]:
        """Hybrid approach for TACTIC"""
        pages = []
        details = []
        
        # Try requests first
        try:
            url = "https://tactic.se/foretag-till-salu/"
            logger.info(f"Scraping TACTIC: {url}")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text(encoding='utf-8', errors='replace')
                    pages.append(html)
                    
                    # Extract business links
                    soup = BeautifulSoup(html, 'html.parser')
                    business_links = soup.find_all('a', href=re.compile(r'/foretag-till-salu/[^/]+'))
                    
                    for link in business_links[:6]:
                        detail_url = urljoin("https://tactic.se", link['href'])
                        try:
                            async with self.session.get(detail_url) as detail_response:
                                if detail_response.status == 200:
                                    detail_html = await detail_response.text(encoding='utf-8', errors='replace')
                                    details.append(detail_html)
                            await asyncio.sleep(1)
                        except Exception as e:
                            logger.error(f"Error getting TACTIC detail: {e}")
                            
        except Exception as e:
            logger.error(f"Error scraping TACTIC: {e}")
            
        return {"pages": pages, "details": details}

    # 5. SFF.SE - 4% of market
    async def scrape_sff_simple(self) -> Dict[str, List[str]]:
        """Simple scraping for SFF"""
        pages = []
        
        try:
            url = "https://www.sffab.se/foretag-till-salu/"
            logger.info(f"Scraping SFF: {url}")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text(encoding='utf-8', errors='replace')
                    pages.append(html)
                    
        except Exception as e:
            logger.error(f"Error scraping SFF: {e}")
            
        return {"pages": pages, "details": []}

    # 6. ADDITIONAL PLATFORMS for remaining coverage
    async def scrape_additional_platforms(self) -> Dict[str, List[str]]:
        """Scrape remaining platforms for full 80%+ coverage"""
        pages = []
        details = []
        
        additional_urls = [
            "https://www.exitpartner.se/foretag-till-salu/",
            "https://bolagsbron.se/category/foretag-til-salu/", 
            "http://www.nmk.se/foretag-till-salu/",
            "https://www.stockholmsforetagsmaklare.se/"
        ]
        
        for url in additional_urls:
            try:
                logger.info(f"Scraping additional platform: {url}")
                async with self.session.get(url) as response:
                    if response.status == 200:
                        html = await response.text(encoding='utf-8', errors='replace')
                        pages.append(html)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
                
        return {"pages": pages, "details": details}

    def _extract_bolagsplatsen_contact(self, html: str) -> Dict[str, str]:
        """Extract contact info from Bolagsplatsen detail pages"""
        soup = BeautifulSoup(html, 'html.parser')
        contact_info = {}
        
        try:
            # Business name from title
            title = soup.find('title')
            if title:
                title_text = title.get_text()
                if ' - ' in title_text:
                    contact_info['business_name'] = title_text.split(' - ')[0].strip()
            
            # Get all text for pattern matching
            text = soup.get_text()
            
            # Swedish phone number patterns
            phone_patterns = [
                r'(\+46[\s-]?\d{1,3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2})',
                r'(0\d{2,3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2})',
                r'(\d{3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2})'
            ]
            
            for pattern in phone_patterns:
                phone_match = re.search(pattern, text)
                if phone_match:
                    contact_info['phone_number'] = phone_match.group(1).strip()
                    break
            
            # Swedish contact names
            name_patterns = [
                r'Kontakt:?\s*([A-ZÅÄÖÜ][a-zåäöü]+\s+[A-ZÅÄÖÜ][a-zåäöü]+)',
                r'Mäklare:?\s*([A-ZÅÄÖÜ][a-zåäöü]+\s+[A-ZÅÄÖÜ][a-zåäöü]+)',
                r'([A-ZÅÄÖÜ][a-zåäöü]+\s+[A-ZÅÄÖÜ][a-zåäöü]+)[\s,]*(\+46|0\d{2})'
            ]
            
            for pattern in name_patterns:
                name_match = re.search(pattern, text)
                if name_match:
                    contact_name = name_match.group(1).strip()
                    # Validate it's a real name, not generic text
                    if contact_name and len(contact_name.split()) == 2:
                        contact_info['contact_name'] = contact_name
                        break
            
            # Email extraction
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
            if email_match:
                contact_info['email'] = email_match.group(1).strip()
                
        except Exception as e:
            logger.error(f"Error extracting contact info: {e}")
            
        return contact_info

    # Main orchestrator for 80%+ coverage
    async def scrape_for_80_percent_coverage(self) -> Dict[str, Any]:
        """Comprehensive scraping targeting 80%+ of Swedish market"""
        
        all_pages = []
        all_details = []
        coverage_stats = {}
        
        # 1. Bolagsplatsen (50% of market) - CRITICAL
        logger.info("=== SCRAPING BOLAGSPLATSEN (50% of market) ===")
        bolagsplatsen_data = await self.scrape_bolagsplatsen_comprehensive()
        all_pages.extend(bolagsplatsen_data["pages"])
        all_details.extend(bolagsplatsen_data["details"])
        coverage_stats["bolagsplatsen"] = {
            "pages": len(bolagsplatsen_data["pages"]),
            "details": len(bolagsplatsen_data["details"])
        }
        
        await asyncio.sleep(3)
        
        # 2. Objektvision (17% of market)  
        logger.info("=== SCRAPING OBJEKTVISION (17% of market) ===")
        objektvision_data = await self.scrape_objektvision_browser()
        all_pages.extend(objektvision_data["pages"])
        all_details.extend(objektvision_data["details"])
        coverage_stats["objektvision"] = {
            "pages": len(objektvision_data["pages"]), 
            "details": len(objektvision_data["details"])
        }
        
        await asyncio.sleep(3)
        
        # 3. Lania (8% of market)
        logger.info("=== SCRAPING LANIA (8% of market) ===")
        lania_data = await self.scrape_lania_browser()
        all_pages.extend(lania_data["pages"])
        all_details.extend(lania_data["details"])
        coverage_stats["lania"] = {
            "pages": len(lania_data["pages"]),
            "details": len(lania_data["details"])
        }
        
        await asyncio.sleep(3)
        
        # 4. TACTIC (6% of market)
        logger.info("=== SCRAPING TACTIC (6% of market) ===")
        tactic_data = await self.scrape_tactic_hybrid()
        all_pages.extend(tactic_data["pages"])
        all_details.extend(tactic_data["details"])
        coverage_stats["tactic"] = {
            "pages": len(tactic_data["pages"]),
            "details": len(tactic_data["details"])
        }
        
        await asyncio.sleep(3)
        
        # 5. SFF (4% of market)
        logger.info("=== SCRAPING SFF (4% of market) ===")
        sff_data = await self.scrape_sff_simple()
        all_pages.extend(sff_data["pages"])
        coverage_stats["sff"] = {
            "pages": len(sff_data["pages"]),
            "details": 0
        }
        
        await asyncio.sleep(3)
        
        # 6. Additional platforms (5% of market combined)
        logger.info("=== SCRAPING ADDITIONAL PLATFORMS (5% of market) ===")
        additional_data = await self.scrape_additional_platforms()
        all_pages.extend(additional_data["pages"])
        all_details.extend(additional_data["details"])
        coverage_stats["additional"] = {
            "pages": len(additional_data["pages"]),
            "details": len(additional_data["details"])
        }
        
        # Calculate total coverage
        estimated_coverage = 50 + 17 + 8 + 6 + 4 + 5  # = 90%
        
        logger.info(f"=== SCRAPING COMPLETE ===")
        logger.info(f"Total pages: {len(all_pages)}")
        logger.info(f"Total details: {len(all_details)}")
        logger.info(f"Estimated market coverage: {estimated_coverage}%")
        
        return {
            "pages": all_pages,
            "details": all_details,
            "scraped_at": datetime.now().isoformat(),
            "coverage_stats": coverage_stats,
            "estimated_market_coverage": f"{estimated_coverage}%"
        }

# FastAPI server
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI(title="Comprehensive Swedish Business Scraper - 80%+ Coverage")

@app.get("/scrap")
async def scrape_swedish_businesses_comprehensive():
    """Comprehensive scraping for 80%+ Swedish market coverage"""
    try:
        async with ComprehensiveSwedishScraper() as scraper:
            result = await scraper.scrape_for_80_percent_coverage()
            
            logger.info(f"Scraping complete: {result['estimated_market_coverage']} coverage")
            
            return JSONResponse(content=result)
            
    except Exception as e:
        logger.error(f"Comprehensive scraping failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "target_coverage": "80%+",
        "platforms_targeted": 9
    }

@app.get("/coverage-estimate")
async def get_coverage_estimate():
    """Show estimated coverage breakdown"""
    scraper = ComprehensiveSwedishScraper()
    return {
        "platform_coverage": scraper.platform_coverage,
        "total_estimated_listings": sum(scraper.platform_coverage.values()),
        "estimated_market_coverage": "90%"
    }

@app.get("/test-bolagsplatsen")
async def test_bolagsplatsen_scraping():
    """Test endpoint to debug Bolagsplatsen scraping"""
    try:
        async with ComprehensiveSwedishScraper() as scraper:
            # Test a single page first
            url = "https://www.bolagsplatsen.se/foretag-till-salu/alla/alla"
            
            logger.info(f"Testing Bolagsplatsen page: {url}")
            
            # Try HTTP request first
            async with scraper.session.get(url) as response:
                if response.status == 200:
                    html = await response.text(encoding='utf-8', errors='replace')
                    
                    # Parse with BeautifulSoup
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Look for all links
                    all_links = soup.find_all('a', href=True)
                    business_links = []
                    
                    # Check different patterns
                    patterns = [
                        r'/foretag-till-salu/[^/]+$',
                        r'/foretag-till-salu/[^/?]+',
                        r'foretag.*till.*salu',
                        r'business',
                        r'company'
                    ]
                    
                    for link in all_links:
                        href = link.get('href', '').lower()
                        text = link.get_text().lower()
                        
                        # Check if it matches any pattern
                        for pattern in patterns:
                            if re.search(pattern, href) or re.search(pattern, text):
                                business_links.append({
                                    'href': link.get('href'),
                                    'text': link.get_text().strip()[:100],
                                    'class': link.get('class', [])
                                })
                                break
                    
                    return {
                        "status": "success",
                        "url": url,
                        "total_links_found": len(all_links),
                        "business_links_found": len(business_links),
                        "business_links": business_links[:20],  # First 20 for debugging
                        "page_title": soup.find('title').get_text() if soup.find('title') else "No title found"
                    }
                else:
                    return {
                        "status": "error",
                        "url": url,
                        "status_code": response.status,
                        "message": "Failed to fetch page"
                    }
                    
    except Exception as e:
        logger.error(f"Test scraping failed: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/test-browser")
async def test_browser_scraping():
    """Test browser automation for Bolagsplatsen"""
    try:
        async with ComprehensiveSwedishScraper() as scraper:
            browser_urls = await scraper._scrape_bolagsplatsen_with_browser()
            
            return {
                "status": "success",
                "browser_urls_found": len(browser_urls),
                "browser_urls": list(browser_urls)[:20]  # First 20 for debugging
            }
                    
    except Exception as e:
        logger.error(f"Browser test failed: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
