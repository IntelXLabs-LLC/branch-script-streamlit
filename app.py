import streamlit as st
import requests
import pandas as pd
import time
from io import StringIO
from urllib.parse import urlparse, parse_qs

st.set_page_config(page_title="Offline Conversions Generator", layout="wide")
st.title("🎯 Google Offline Conversions Generator")

st.markdown(
    """
    1. Paste your presigned S3 URLs (one per line).  
    2. Click **Run**.  
    3. When it finishes, download your filtered CSV.
    """
)

urls_input = st.text_area("Presigned CSV.gz URLs", height=200)
chunksize = st.number_input("Pandas chunksize", min_value=10_000, value=100_000, step=10_000)
run = st.button("▶️ Run")

if run:
    urls = [u.strip() for u in urls_input.splitlines() if u.strip()]
    if not urls:
        st.error("Please paste at least one URL.")
    else:
        output_buf = StringIO()
        # write header
        output_buf.write("Google Click ID,Conversion Name,Conversion Time,Conversion Value,Conversion Currency\n")

        progress = st.progress(0)
        status = st.empty()

        for idx, url in enumerate(urls, start=1):
            status.text(f"Processing URL {idx}/{len(urls)}")
            # fetch with retry
            resp = None
            for attempt in range(3):
                try:
                    resp = requests.get(url, stream=True, timeout=60)
                    resp.raise_for_status()
                    break
                except Exception as e:
                    time.sleep(5)
            if resp is None:
                st.warning(f"  ❌ Failed to download URL #{idx}")
                continue

            # stream through pandas
            reader = pd.read_csv(
                resp.raw,
                compression="gzip",
                usecols=[
                    "name",
                    "timestamp_iso",
                    "event_data_revenue",
                    "event_data_currency",
                    "last_attributed_touch_data_plus_url",
                ],
                dtype={
                    "name": str,
                    "timestamp_iso": str,
                    "event_data_revenue": float,
                    "event_data_currency": str,
                    "last_attributed_touch_data_plus_url": str,
                },
                chunksize=chunksize,
                low_memory=False,
            )

            for chunk in reader:
                # extract gclid
                chunk["gclid"] = chunk["last_attributed_touch_data_plus_url"].apply(
                    lambda u: parse_qs(urlparse(u).query).get("gclid", [""])[0]
                    if isinstance(u, str)
                    else ""
                )
                # default revenue
                chunk["event_data_revenue"] = chunk["event_data_revenue"].fillna(1)
                # default currency
                chunk["event_data_currency"] = chunk["event_data_currency"].fillna("INR").replace("", "INR")
                # filter
                filt = chunk[chunk["gclid"] != ""]
                if not filt.empty:
                    out = (
                        filt.rename(
                            columns={
                                "gclid": "Google Click ID",
                                "name": "Conversion Name",
                                "timestamp_iso": "Conversion Time",
                                "event_data_revenue": "Conversion Value",
                                "event_data_currency": "Conversion Currency",
                            }
                        )[
                            [
                                "Google Click ID",
                                "Conversion Name",
                                "Conversion Time",
                                "Conversion Value",
                                "Conversion Currency",
                            ]
                        ]
                    )
                    out.to_csv(output_buf, header=False, index=False)

            progress.progress(idx / len(urls))

        status.text("✅ All done!")
        st.download_button(
            "📥 Download CSV",
            data=output_buf.getvalue(),
            file_name="offline_conversions.csv",
            mime="text/csv",
        )