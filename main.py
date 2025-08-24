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
        
        # Add a simple test first
        test_response = {"status": "starting_scrape", "target_url": TARGET_URL}
        logger.info(f"Test response: {test_response}")
        
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
        logger.info(f"Response content length: {len(response.content)}")
        
        # Parse HTML content using BeautifulSoup with html.parser (built-in)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Debug: Check if we can find any content
        logger.info(f"Page title: {soup.title.string if soup.title else 'No title found'}")
        
        # Find all business listing containers - looking for the actual structure
        # The page shows PREMIUMANNONS links which contain the business listings
        business_links = soup.find_all('a', href=lambda x: x and '/foretag-till-salu/' in x and x != '/foretag-till-salu/')
        logger.info(f"Found {len(business_links)} business listing links")
        
        # Extract data from each business listing
        business_listings = []
        
        for i, link in enumerate(business_links):
            try:
                logger.info(f"Processing listing {i}")
                
                # Get the href for listing_url
                listing_url = link.get('href', '')
                logger.info(f"Found URL: {listing_url}")
                
                # Find the parent container that holds all the information
                parent_container = link.parent
                if not parent_container:
                    continue
                
                # Look for title - it could be in the link text or nearby
                title_text = link.get_text(strip=True)
                if not title_text:
                    # Try to find title in nearby elements
                    title_elements = parent_container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    if title_elements:
                        title_text = title_elements[0].get_text(strip=True)
                
                logger.info(f"Found title: {title_text}")
                
                # Look for location and industry information in the text around the link
                # Based on the page structure, we need to parse the surrounding text
                container_text = parent_container.get_text(separator=' | ', strip=True)
                logger.info(f"Container text: {container_text[:200]}...")
                
                # Extract location and industry from the container text
                # This is a simplified extraction - you may need to refine based on actual patterns
                location = ""
                industry = ""
                
                # Look for common Swedish county/region names
                swedish_regions = ['Stockholm', 'Göteborg', 'Malmö', 'Västra Götaland', 'Skåne', 'Jämtland', 
                                 'Örebro', 'Kronoberg', 'Södermanland', 'Västerås', 'Eskilstuna', 'Sverige']
                
                for region in swedish_regions:
                    if region in container_text:
                        location = region
                        break
                
                # Look for industry keywords in the beginning of the text
                text_parts = container_text.split()
                if len(text_parts) > 0:
                    # First few words might indicate industry
                    potential_industry = ' '.join(text_parts[:3])
                    if any(word in potential_industry.lower() for word in ['e-handel', 'tillverkning', 'handel', 'bygg', 'restaurang', 'konditori', 'bageri']):
                        industry = potential_industry
                
                logger.info(f"Found location: {location}")
                logger.info(f"Found industry: {industry}")
                
                # Only add listings that have at least a title and URL
                if title_text and listing_url:
                    business_listing = {
                        "title": title_text,
                        "location": location,
                        "industry": industry,
                        "listing_url": listing_url
                    }
                    business_listings.append(business_listing)
                    logger.info(f"Added listing: {business_listing}")
                
                # Limit to first 20 listings to avoid timeout
                if len(business_listings) >= 20:
                    break
                    
            except Exception as e:
                # Log individual parsing errors but continue processing other listings
                logger.warning(f"Error parsing individual listing {i}: {str(e)}")
                continue
        
        logger.info(f"Successfully parsed {len(business_listings)} business listings")
        
        # If no listings found, return debug info
        if len(business_listings) == 0:
            return [{
                "title": "DEBUG: No listings found",
                "location": f"Page title: {soup.title.string if soup.title else 'No title'}",
                "industry": f"Total div elements: {len(soup.find_all('div'))}",
                "listing_url": f"Response length: {len(response.content)}"
            }]
        
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
    import os
    
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
