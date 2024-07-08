# imports
import streamlit as st
import json
from datetime import datetime
import sounddevice as sd
import soundfile as sf
from scipy.io.wavfile import write
import os
from datetime import datetime
from main import run, fetch_final_dataframe
import pandas as pd
import pickle
from elastic_search_helper import ESmanager


def start_recording():
    st.session_state.current_status = "Running"
    st.session_state.recording = sd.rec(int(20 * 48000), samplerate=48000, channels=1)


def stop_recording():
    st.session_state.is_clicked = False
    sd.stop()
    st.session_state.current_timestamp_string = datetime.now().strftime(
        "%d-%m-%Y_%H-%M-%S"
    )
    current_log_path = "./logs/" + st.session_state.current_timestamp_string

    if not os.path.exists(current_log_path):
        os.makedirs(current_log_path)

    write(f"{current_log_path}/audio.wav", 48000, st.session_state.recording)

    # Extract audio data and sampling rate from file
    data, fs = sf.read(f"{current_log_path}/audio.wav",)
    # Save as FLAC file at correct sampling rate
    sf.write(f"{current_log_path}/audio.flac", data, fs)
    st.session_state.current_status = "Done"


if __name__ == "__main__":

    if "current_status" not in st.session_state:
        st.session_state.current_status = "None"
        st.session_state.is_clicked = None
        st.session_state.is_clicked_2 = None

    # start streamlit session
    st.title("Multi-Lingual Voice Search of News Articles")
    
    # trigger pipeline function
    button_placeholder = st.empty()
    with button_placeholder.container():
        st.session_state.is_clicked = st.button("Start Voice Search")

    execution_placeholder = st.empty()
    if st.session_state.is_clicked:
        button_placeholder.empty()
        start_recording()
        with execution_placeholder.container():
            st.session_state.is_clicked_2 = st.button(
                "Stop Voice Search", on_click=stop_recording
            )

    if st.session_state.is_clicked_2:
        execution_placeholder.empty()
        st.session_state.is_clicked_2 = False

    # st.write(st.session_state.current_status)
    # st.write(st.session_state.is_clicked)
    # st.write(st.session_state.is_clicked_2)

    if st.session_state.current_status == "Done":
        text, lang = run(st.session_state.current_timestamp_string)
        st.success(f"Text Detected - {text}")
        # st.success(f"Language Detected - {lang}")

        df = None
        with st.spinner("Fetching Articles...."):
            try:
                df = fetch_final_dataframe(text)
            except Exception as e:
                st.write(f"Elastic Search Connection Error - {e}")
            # st.write(str(len(df)))

            if df is not None:
                # threshold = st.slider('Similarity Threshold', 0, 100, 30)
                # df = df[df.score >= threshold]
                available_langs = df.text_lang.unique()
                if len(available_langs) != 0:
                    language_filter = st.multiselect(
                        "Filter Languages", available_langs, available_langs
                    )
                    df = df[df.text_lang.isin(language_filter)]
                st.balloons()

                if df.shape[0]:
                    for idx, row in df.iterrows():
                        st.header(row["title_articles"])
                        st.markdown(row['text_articles_1'])
                        with st.expander("Full Article"):
                            st.markdown(row["full_text"])
                else:
                    st.write('No Relevant Articles Found!')

