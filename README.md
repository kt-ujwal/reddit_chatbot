# Reddit Chatbot.
Following [sentdex's tutorial][playlist] on neural machine translation, with some tweaks.

## Usage
Edit parameters in `chatbot_databese.py`.

To get SQL database, run: `python3 chatbot_database.py`.

To get `train.from` and `train.to`, run: `python3 training_data.py`.

## Notes
To save time, `training_data.py` processes all data at once in RAM.

[playlist]: https://www.youtube.com/playlist?list=PLQVvvaa0QuDdc2k5dwtDTyT9aCja0on8j
