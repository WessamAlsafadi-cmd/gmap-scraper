import streamlit as st
import pandas as pd
import json
import os
import requests
from datetime import datetime
from apify_client import ApifyClient
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Get API token from Streamlit secrets (for Streamlit Cloud) or environment
try:
    API_TOKEN = st.secrets["APIFY_API_TOKEN"]
except:
    API_TOKEN = os.getenv("APIFY_API_TOKEN")

# Page configuration
st.set_page_config(
    page_title="Google Maps Business Scraper",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
.stApp { background-color: #f8f9fa; }
h1 { color: #1a73e8; }
/*.stFormSubmitButton > button { 
    background-color: #6B48FF !important; 
    color: white; 
    border-radius: 8px; 
    border: 1px solid #6B48FF !important;  

}
h3#5c826d33{
    color: #1A2E5A !important;      
}
h2, h3{
    color: white !important;            
}
[data-testid="stSidebarContent"], [role="slider"], .st-kc{
    background-color: #1A2E5A !important;           
}
.stFormSubmitButton > button:hover { 
    background-color: #EDEDED !important; 
}*/
.stContainer { 
    border: 1px solid #e0e0e0; 
    border-radius: 8px; 
    padding: 16px; 
}
/*.stExpander { 
    background-color: #ffffff; 
    border-radius: 8px; 
}*/
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'scraping_results' not in st.session_state:
    st.session_state.scraping_results = None
if 'scraping_in_progress' not in st.session_state:
    st.session_state.scraping_in_progress = False

def send_to_webhook(data, webhook_url, send_individually=False):
    """Send scraped data to a webhook"""
    try:
        if send_individually:
            success_count = 0
            failed_count = 0
            
            for index, record in enumerate(data, 1):
                try:
                    payload = {
                        "timestamp": datetime.now().isoformat(),
                        "record_number": index,
                        "total_records": len(data),
                        "data": record
                    }
                    response = requests.post(webhook_url, json=payload, timeout=30)
                    response.raise_for_status()
                    success_count += 1
                    time.sleep(0.1)
                except requests.exceptions.RequestException:
                    failed_count += 1
                    continue
            
            if failed_count == 0:
                return True, f"Successfully sent all {success_count} records individually to webhook"
            else:
                return True, f"Sent {success_count} records successfully, {failed_count} failed"
        
        else:
            payload = {
                "timestamp": datetime.now().isoformat(),
                "total_results": len(data),
                "data": data
            }
            response = requests.post(webhook_url, json=payload, timeout=30)
            response.raise_for_status()
            return True, f"Successfully sent {len(data)} records in bulk to webhook"
            
    except requests.exceptions.RequestException as e:
        return False, f"Webhook error: {str(e)}"

def scrape_google_maps(search_query, location, max_results, additional_options=None):
    """Scrape Google Maps using Apify"""
    try:
        client = ApifyClient(API_TOKEN)
        
        run_input = {
            "searchStringsArray": [search_query],
            "locationQuery": location,
            "maxCrawledPlacesPerSearch": max_results,
            "language": "en",
            "searchMatching": "all",
            "placeMinimumStars": "",
            "website": "allPlaces",
            "skipClosedPlaces": additional_options.get("skipClosedPlaces", False),
            "scrapePlaceDetailPage": additional_options.get("scrapePlaceDetailPage", False),
            "scrapeTableReservationProvider": False,
            "includeWebResults": additional_options.get("includeWebResults", False),
            "scrapeDirectories": False,
            "maxQuestions": 0,
            "scrapeContacts": additional_options.get("scrapeContacts", False),
            "maximumLeadsEnrichmentRecords": additional_options.get("maximumLeadsEnrichmentRecords", 0),
            "maxReviews": additional_options.get("maxReviews", 0),
            "reviewsSort": "newest",
            "reviewsFilterString": "",
            "reviewsOrigin": "all",
            "scrapeReviewsPersonalData": True,
            "maxImages": additional_options.get("maxImages", 0),
            "scrapeImageAuthors": False,
            "allPlacesNoSearchAction": "",
        }
        
        run = client.actor("compass/crawler-google-places").call(run_input=run_input)
        
        results = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)
        
        return True, results, run
    
    except Exception as e:
        return False, str(e), None

# Main UI
st.title("Google Maps Business Scraper")
st.markdown("Scrape business information from Google Maps with ease using Apify's crawler", unsafe_allow_html=True)

# Check if API token is available
if not API_TOKEN:
    st.error("❌ Apify API token not found in environment variables. Please add APIFY_API_TOKEN to your .env file.")
    st.stop()
else:
    # Show API token status (masked for security)
    st.sidebar.success(f"✅ API Token loaded: {'*' * 20}{API_TOKEN[-4:] if len(API_TOKEN) > 4 else '****'}")
    
    # Add a test button to verify the token
    if st.sidebar.button("🔍 Test API Token"):
        try:
            client = ApifyClient(API_TOKEN)
            # Try to get user info to test the token
            user_info = client.user().get()
            st.sidebar.success(f"✅ Token valid for user: {user_info.get('username', 'Unknown')}")
        except Exception as e:
            st.sidebar.error(f"❌ Token test failed: {str(e)}")

# Sidebar for configuration
with st.sidebar:
    st.header("Settings")
    
    with st.container():
        st.subheader("Webhook Configuration")
        webhook_url = st.text_input(
            "Webhook URL (Optional)",
            placeholder="https://your-webhook-endpoint.com/data",
            help="Send results to this URL after scraping"
        )
    
    with st.container():
        st.subheader("Advanced Settings")
        with st.expander("Customize Scraping Options"):
            skip_closed = st.checkbox("Skip Closed Places", value=False)
            scrape_details = st.checkbox("Detailed Place Info", value=False)
            scrape_contacts = st.checkbox("Contact Info", value=False)
            include_web_results = st.checkbox("Web Results", value=False)
            
            max_reviews = st.slider("Max Reviews per Place", 0, 100, 0)
            max_images = st.slider("Max Images per Place", 0, 20, 0)
            max_leads = st.slider("Max Lead Enrichment Records", 0, 100, 0)

# Search Section
with st.container(border=True):
    st.subheader("🔍 Search for Businesses")
    col1, col2 = st.columns([3, 1])

    with col1:
        with st.form("scraper_form"):
            search_query = st.text_input(
                "Search Query",
                placeholder="e.g., dental clinics, restaurants, hotels",
                help="What type of businesses are you looking for?"
            )
            
            location = st.text_input(
                "Location",
                placeholder="e.g., Dubai, UAE or New York, USA",
                help="City, state, or country to search in"
            )
            
            max_results = st.slider(
                "Maximum Results",
                min_value=1,
                max_value=500,
                value=50,
                help="Number of places to scrape (more results = higher cost)"
            )
            
            submit_button = st.form_submit_button("Start Scraping", type="primary", use_container_width=True)

    with col2:
        with st.expander("Quick Stats", expanded=bool(st.session_state.scraping_results)):
            if st.session_state.scraping_results:
                results = st.session_state.scraping_results
                st.metric("Total Results", len(results))
                
                if results:
                    with_ratings = len([r for r in results if r.get('totalScore')])
                    with_phone = len([r for r in results if r.get('phone')])
                    with_website = len([r for r in results if r.get('website')])
                    
                    st.metric("With Ratings", with_ratings)
                    st.metric("With Phone", with_phone)
                    st.metric("With Website", with_website)
            else:
                st.info("Run a search to see stats!")

# Handle form submission
if submit_button and search_query and location:
    if not st.session_state.scraping_in_progress:
        st.session_state.scraping_in_progress = True
        st.session_state.scraping_results = None
        
        additional_options = {
            "skipClosedPlaces": skip_closed,
            "scrapePlaceDetailPage": scrape_details,
            "scrapeContacts": scrape_contacts,
            "includeWebResults": include_web_results,
            "maxReviews": max_reviews,
            "maxImages": max_images,
            "maximumLeadsEnrichmentRecords": max_leads,
        }
        
        with st.status("Scraping in progress...", expanded=True) as status:
            st.write("Connecting to Apify...")
            success, results, run_info = scrape_google_maps(
                search_query, location, max_results, additional_options
            )
            
            if success:
                st.session_state.scraping_results = results
                status.update(label=f"Scraped {len(results)} businesses!", state="complete")
                st.toast(f"Found {len(results)} businesses!", icon="🎉")
                
                if webhook_url:
                    webhook_success, webhook_msg = send_to_webhook(results, webhook_url, send_individually=False)
                    if webhook_success:
                        st.toast(f"📡 {webhook_msg}", icon="✅")
                    else:
                        st.toast(f"❌ {webhook_msg}", icon="⚠️")
            else:
                st.session_state.scraping_in_progress = False
                status.update(label=f"❌ Scraping failed: {results}", state="error")
                st.toast(f"Scraping failed: {results}", icon="⚠️")
elif submit_button:
    if not search_query:
        st.error("❌ Please enter a search query")
    if not location:
        st.error("❌ Please enter a location")

# Display results
if st.session_state.scraping_results:
    results = st.session_state.scraping_results
    
    with st.container(border=True):
        st.subheader("📋 Scraped Businesses")
        
        search_term = st.text_input(
            "Filter Results",
            placeholder="Search by business name or address...",
            key="results_filter"
        )
        
        df = pd.DataFrame(results)
        if search_term:
            df = df[df['title'].str.contains(search_term, case=False, na=False) | 
                    df['address'].str.contains(search_term, case=False, na=False)]
        
        st.data_editor(
            df[['title', 'address', 'totalScore', 'reviewsCount', 'phone', 'website']].fillna('N/A'),
            use_container_width=True,
            column_config={
                "title": st.column_config.TextColumn("Business Name", width="medium"),
                "address": st.column_config.TextColumn("Address", width="large"),
                "totalScore": st.column_config.NumberColumn("Rating", format="%.1f"),
                "reviewsCount": st.column_config.NumberColumn("Reviews"),
                "phone": st.column_config.TextColumn("Phone"),
                "website": st.column_config.LinkColumn("Website", display_text="Visit")
            }
        )
        
        with st.container(border=True):
            st.subheader("Export & Share Results")
            tab1, tab2 = st.tabs(["Download", "Webhook"])
            
            with tab1:
                col1, col2, col3 = st.columns(3)
                with col1:
                    json_data = json.dumps(results, indent=2)
                    st.download_button(
                        label="JSON",
                        data=json_data,
                        file_name=f"{search_query}_{location}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )
                with col2:
                    csv_data = df.to_csv(index=False)
                    st.download_button(
                        label="CSV",
                        data=csv_data,
                        file_name=f"{search_query}_{location}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                with col3:
                    excel_buffer = pd.ExcelWriter('temp.xlsx', engine='openpyxl')
                    df.to_excel(excel_buffer, index=False, sheet_name='Results')
                    excel_buffer.close()
                    with open('temp.xlsx', 'rb') as f:
                        excel_data = f.read()
                    st.download_button(
                        label="Excel",
                        data=excel_data,
                        file_name=f"{search_query}_{location}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    if os.path.exists('temp.xlsx'):
                        os.remove('temp.xlsx')
            
            with tab2:
                webhook_col1, webhook_col2 = st.columns([3, 1])
                with webhook_col1:
                    webhook_url_main = st.text_input(
                        "Webhook URL",
                        placeholder="https://your-webhook-endpoint.com/data",
                        help="Send results to this URL",
                        key="main_webhook"
                    )
                with webhook_col2:
                    send_individually = st.checkbox(
                        "Send Individually",
                        help="Send each record separately"
                    )
                
                if send_individually:
                    st.info(f"Sending {len(results)} separate requests")
                else:
                    st.info(f"Sending all {len(results)} records in one request")
                
                if st.button(
                    f"📡 Send to Webhook {'(Individual)' if send_individually else '(Bulk)'}",
                    type="primary",
                    use_container_width=True,
                    disabled=not webhook_url_main
                ):
                    with st.spinner(f"Sending {len(results)} records..."):
                        webhook_success, webhook_msg = send_to_webhook(results, webhook_url_main, send_individually)
                        if webhook_success:
                            st.success(f"{webhook_msg}")
                        else:
                            st.error(f"{webhook_msg}")
                
                if webhook_url_main:
                    with st.expander("Preview Payload"):
                        preview_payload = {
                            "timestamp": datetime.now().isoformat(),
                            "total_results": len(results),
                            "data": results[:2] + ["..."] if len(results) > 2 else results
                        } if not send_individually else {
                            "timestamp": datetime.now().isoformat(),
                            "record_number": 1,
                            "total_records": len(results),
                            "data": results[0] if results else {}
                        }
                        st.json(preview_payload)

else:
    st.info("No results found. Try adjusting your search parameters.")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>Powered by Apify Google Maps Scraper | Built with Streamlit</p>
    </div>
    """,
    unsafe_allow_html=True
)
