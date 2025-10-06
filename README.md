# 📂 Folder Tree Viewer

Here’s an app I coded — it shows all files in a selected folder in a clean **tree format**.  
Originally, I made it while working on my *SnowRunner Save Editor* to help visualize file structures.  
I ended up not using it, but since it might be useful to someone else, here it is.  

---

## 🧩 Features

- 📁 **Tree View of Files and Folders**  
  Displays all contents of a selected directory in a collapsible, expandable tree structure.

- 🌈 **Extension-Based Color Highlighting**  
  Automatically color-codes file and folder names based on their extensions for easier navigation.

- 🔍 **Search Function**  
  Quickly search through all files and folders (even in unexpanded directories) with real-time results.

- 📦 **Folder Size Calculation**  
  Calculates and displays folder sizes (in proper units like KB, MB, GB) with a progress bar and live updates.

- 📊 **Expand / Collapse All**  
  Instantly expand or collapse the entire folder tree — with smooth background processing and a working status indicator.

- ⚡ **Precount Mode**  
  Optionally preloads item counts for faster browsing.

- 📂 **Multi-Selection and Copy Support**  
  Click + drag or use Ctrl / Shift to select multiple files or folders — just like in Windows Explorer.

- 🖌️ **Custom Highlight Colors**  
  Change colors for specific file extensions directly from the Settings menu.

- 💾 **Persistent Settings**  
  The app remembers your preferences between runs.

- 🧠 **Threaded Operations**  
  Searching, expanding, and size calculations all run in background threads to keep the UI responsive.

- 🔒 **Crash Protection & Error Handling**  
  Built with stability in mind — handles large directories and unexpected permission errors gracefully.

---

## 🧰 How to Use

1. **Download the latest release:**  
   👉 [Releases Page](../../releases/latest)

2. **Run the app:**  
   Just open the `.exe` file — no install needed.

3. **Browse a folder or drive:**  
   Click *Browse*, or select one of the drives under *This PC*.

4. **Explore, search, and visualize your files.**

---

## ⚙️ Building from Source

If you want to build or modify it yourself:

```bash
pip install PySide6 psutil
python tree.py
