import os
import ffmpeg
import re
from datetime import datetime
from pymongo import MongoClient
import gridfs
import whisper
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

# -----------------------------
# MongoDB Atlas Connection Setup
# -----------------------------
client = MongoClient("mongodb+srv://ajitha:smily332@cluster0.mbuvlpt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["video_db"]
fs = gridfs.GridFS(db)
collection = db["batch_04"]

# -----------------------------
# Load Whisper Model
# -----------------------------
model = whisper.load_model("base")

# -----------------------------
# Helper Functions
# -----------------------------
def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename).replace(" ", "_")

def compress_video(input_file):
    output_file = f"compressed_{sanitize_filename(os.path.basename(input_file))}.mp4"
    try:
        (
            ffmpeg
            .input(input_file)
            .output(output_file, vcodec="libx264", preset="slow", crf=23)
            .run(overwrite_output=True)
        )
        return output_file
    except ffmpeg._run.Error as e:
        print(f"❌ Compression error: {e.stderr.decode()}")
        return None

def extract_audio(video_file):
    audio_file = video_file.replace(".mp4", ".wav")
    (
        ffmpeg
        .input(video_file)
        .output(audio_file, acodec='pcm_s16le', ar='16000')
        .run(overwrite_output=True)
    )
    return audio_file

def transcribe_audio(audio_path):
    print("🧠 Transcribing audio...")
    result = model.transcribe(audio_path)
    return result["text"]

def upload_to_mongodb(file_name, video_path, transcription):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(video_path, "rb") as f:
        video_id = fs.put(f, filename=file_name)

    collection.insert_one({
        "file_name": file_name,
        "video_id": video_id,
        "upload_datetime": current_datetime,
        "transcription": transcription
    })

    print(f"✅ Uploaded to MongoDB Atlas: {file_name} (Video ID: {video_id})")
    os.remove(video_path)

def process_video(video_path):
    if not os.path.exists(video_path):
        print(f"❌ File not found: {video_path}")
        return

    file_name = os.path.basename(video_path)
    print(f"\n🎬 Processing: {file_name}")
    compressed = compress_video(video_path)

    if compressed:
        audio = extract_audio(compressed)
        transcription = transcribe_audio(audio)
        upload_to_mongodb(file_name, compressed, transcription)
        os.remove(audio)

# -----------------------------
# Main Logic
# -----------------------------

if __name__ == "__main__":
    # ✅ Predefined path — your video file
    input_video_path = r"C:\Users\admin\Downloads\2.Hana services intro (16-04-2025)"

    # Process the given video
    process_video(input_video_path)
