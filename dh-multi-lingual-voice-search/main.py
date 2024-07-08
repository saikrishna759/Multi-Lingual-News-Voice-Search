import logging
from datetime import datetime
from google.cloud import speech
from google.cloud import translate_v2 as translate
from elastic_search_helper import ESmanager
import six
import io, os, sys


def transcribe_file(speech_file, target_lang="en-IN"):
    """Transcribe the given audio file."""
    client = speech.SpeechClient()

    with io.open(speech_file, "rb") as audio_file:
        content = audio_file.read()

    audio = speech.RecognitionAudio(content=content)
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
        language_code=target_lang,
        sample_rate_hertz=48000,
        audio_channel_count=1,
        use_enhanced=True,
    )

    response = client.recognize(config=config, audio=audio)

    # # Each result is for a consecutive portion of the audio. Iterate through
    # # them to get the transcripts for the entire audio file.
    # for result in response.results:
    #     # The first alternative is the most likely one for this portion.
    #     print(u"Transcript: {}".format(result.alternatives[0].transcript))

    top_result = response.results[0].alternatives[0]
    text_converted, confidence = top_result.transcript, top_result.confidence
    return text_converted, confidence


def detect_language(text):
    """Detect Language in the given text"""

    translate_client = translate.Client()
    detected_language_dict = translate_client.detect_language(text)
    detected_language = detected_language_dict["language"]
    confidence = detected_language_dict["confidence"]
    return detected_language, confidence


def translate_text(target, text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """

    translate_client = translate.Client()

    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")

    # Text can also be a sequence of strings, in which case this method
    # will return a sequence of results for each text.

    # print(translate_client.detect_language(text))
    result = translate_client.translate(text, target_language=target)

    # print(u"Text: {}".format(result["input"]))
    # print(u"Translation: {}".format(result["translatedText"]))
    # print(u"Detected source language: {}".format(result["detectedSourceLanguage"]))

    return result["translatedText"]


def run(current_timestamp_string):

    current_log_path = "./logs/" + current_timestamp_string

    if not os.path.exists(current_log_path):
        os.makedirs(current_log_path)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    handler = logging.FileHandler(f"{current_log_path}/run.log")
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(handler)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/saikrishna/Downloads/et-gcp-aiml-sandbox-0bc6ef830e1f.json'

    try:
        transcribed_text, confidence = transcribe_file(f"{current_log_path}/audio.flac")
        logger.info(
            f"Transcribed output -> {transcribed_text} | confidence: {confidence}"
        )

        detected_language, confidence = detect_language(transcribed_text)
        logger.info(
            f"Language Detected -> {detected_language} | confidence: {confidence}"
        )
        # if detected_language != 'en':
        #     transcribed_text, confidence = transcribe_file(f'{current_log_path}/audio.flac', 'hi-IN')

        translated_output = translate_text("en", transcribed_text)
        logger.info(f"Target Translated output -> {translated_output}")

        return transcribed_text, detected_language
    except Exception as e:
        logger.error(f"Error transcribing -> {e}")

    return "", ""


def fetch_final_dataframe(text, limit=100):
    em_client = ESmanager.get_instance()
    data1 = em_client.get_matching_articles(text, "dh-news-n-gram-index-12", limit)
    data2 = em_client.get_matching_articles(text, "dh-news-whitespace-index-10", limit)

    df = em_client.make_dataframe(data1[0], data1[1], data2[0], data2[1])

    df = df.drop_duplicates()
    # print(df.score.min(), df.score.max())
    
    return df


if __name__ == "__main__":
    current_timestamp_string = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

    run(current_timestamp_string)


"""
Issues Detected:
    Latest Index improper clustering
    Low scores
"""
