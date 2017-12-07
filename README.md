# Reddit Chatbot
Following [sentdex's tutorial][playlist] on neural machine translation, with some tweaks.

## Usage
1. `git clone --recursive https://github.com/zhengqunkoo/reddit_chatbot.git`
2. Edit parameters in `chatbot_databese.py` and `training_data.py`.
3. To get SQL database, run: `python3 chatbot_database.py`.
4. To get `train.from` and `train.to`, run: `python3 training_data.py`.
5. Follow [nmt-chatbot instructions][nmt-chatbot]

## Notes
For one month of reddit comments (53gb JSON), `chatbot_database.py` takes about 30 minutes, to produce a 1.6gb database.
To save time, `training_data.py` processes about 1 million rows of data in 10 seconds with about 512mb of RAM.

[playlist]: https://www.youtube.com/playlist?list=PLQVvvaa0QuDdc2k5dwtDTyT9aCja0on8j
[nmt-chatbot]: https://github.com/daniel-kukiela/nmt-chatbot
