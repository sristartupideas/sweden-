"""
FastAPI Web Scraper for Swedish Business Listings
Scrapes business listings from bolagsplatsen.se and returns structured JSON data.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Swedish Business Listings Scraper",
    description="API to scrape business listings from bolagsplatsen.se",
    version="1.0.0"
)

# Target URL for scraping
TARGET_URL = "https://www.bolagsplatsen.se/foretag-till-salu"

@app.get("/scrape", response_model=List[Dict[str, str]])
async def scrape_business_listings():
    """
    Scrape business listings from bolagsplatsen.se
    
    Returns:
        List[Dict]: List of business listings with title, location, industry, and listing_url
        
    Raises:
        HTTPException: If scraping fails due to network issues or parsing errors
    """
    try:
        # Make asynchronous HTTP request to target website
        logger.info(f"Starting scrape request to {TARGET_URL}")
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Add headers to mimic a real browser request
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "sv-SE,sv;q=0.8,en-US;q=0.5,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive"
            }
            
            response = await client.get(TARGET_URL, headers=headers)
            response.raise_for_status()  # Raise exception for HTTP error status codes
            
        logger.info(f"Successfully fetched page content. Status code: {response.status_code}")
        
        # Parse HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all business listing containers using the specified CSS selector
        business_containers = soup.select('.objekt')
        logger.info(f"Found {len(business_containers)} business listings")
        
        # Extract data from each business listing
        business_listings = []
        
        for container in business_containers:
            try:
                # Extract title from h5 tag
                title_element = container.find('h5')
                title = title_element.get_text(strip=True) if title_element else ""
                
                # Extract listing URL from anchor tag
                link_element = container.find('a')
                listing_url = link_element.get('href', '') if link_element else ""
                
                # Find all elements with class 'information'
                info_elements = container.find_all(class_='information')
                
                # Extract location (first .information element) and industry (last .information element)
                location = ""
                industry = ""
                
                if info_elements:
                    # Location is the first .information element
                    location = info_elements[0].get_text(strip=True)
                    
                    # Industry is the last .information element
                    if len(info_elements) > 1:
                        industry = info_elements[-1].get_text(strip=True)
                    else:
                        # If only one .information element, it might be the location or industry
                        # Based on the structure, we'll assume it's the location
                        industry = ""
                
                # Only add listings that have at least a title
                if title:
                    business_listing = {
                        "title": title,
                        "location": location,
                        "industry": industry,
                        "listing_url": listing_url
                    }
                    business_listings.append(business_listing)
                    
            except Exception as e:
                # Log individual parsing errors but continue processing other listings
                logger.warning(f"Error parsing individual listing: {str(e)}")
                continue
        
        logger.info(f"Successfully parsed {len(business_listings)} business listings")
        
        # Return the scraped data as JSON
        return business_listings
        
    except httpx.HTTPError as e:
        # Handle HTTP-related errors
        logger.error(f"HTTP error occurred: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to fetch data from target website",
                "message": f"HTTP error: {str(e)}"
            }
        )
        
    except httpx.TimeoutException as e:
        # Handle timeout errors
        logger.error(f"Request timeout: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Request timeout",
                "message": "The target website took too long to respond"
            }
        )
        
    except Exception as e:
        # Handle any other unexpected errors
        logger.error(f"Unexpected error occurred: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal server error",
                "message": f"An unexpected error occurred: {str(e)}"
            }
        )

@app.get("/")
async def root():
    """
    Root endpoint providing API information
    """
    return {
        "message": "Swedish Business Listings Scraper API",
        "endpoints": {
            "/scrape": "GET - Scrape business listings from bolagsplatsen.se"
        },
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy", "service": "scraper-api"}

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
