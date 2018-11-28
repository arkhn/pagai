# Example of LSTM to learn a sequence
from keras.models import Sequential
from keras.layers import Dropout, Activation, LSTM, Dense
from keras.layers.embeddings import Embedding
from keras.preprocessing.text import Tokenizer
from sklearn.preprocessing import LabelEncoder
from keras.utils import to_categorical
import numpy as np

class RNNClassifier:
    def __init__(self, max_len_sequence, num_classes, name_classes, model_type):
        self.max_len_sequence = max_len_sequence
        self.num_classes = num_classes
        self.name_classes = name_classes
        self.model_type = model_type

    def built_dict_character(self, corpus):
        tokenizer = Tokenizer(char_level=True, lower=False)
        tokenizer.fit_on_texts(corpus)
        #Unknow character is n+1
        self.vocab_size = len(tokenizer.word_index)+1
        print("Vocabulary size is: {}".format(self.vocab_size))
        self.tokenizer = tokenizer

    def str_to_matrix(self, X_str):
        X_matrix = np.zeros((len(X_str), self.max_len_sequence, self.vocab_size))
        for i in np.arange(len(X_str)):
            sequence_i = self.tokenizer.texts_to_matrix(X_str[i])
            #X_matrix[i, : np.min(sequence_i.shape[0],self.max_len_sequence), :] = sequence_i[: np.min(sequence_i.shape[0],self.max_len_sequence), : ]
            X_matrix[i, : sequence_i.shape[0], :] = sequence_i[ : self.max_len_sequence,:]

        return X_matrix

    def buil_model(self, use_dropout=True, hidden_size=5, dropout=0.5):
        model = Sequential()
        # batch size, number of time steps, hidden size)
        #model.add(Embedding(input_dim=self.vocab_size, output_dim=hidden_size, input_length=self.max_len_sequence))
        model.add(LSTM(hidden_size, input_shape=(self.max_len_sequence, self.vocab_size), return_sequences=False))
        #model.add(LSTM(hidden_size, return_sequences=True))
        if use_dropout:
            model.add(Dropout(dropout))
        #model.add(TimeDistributed(Dense(self.vocab_size)))
        model.add(Dense(self.num_classes, activation='softmax'))
        model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['categorical_accuracy'])
        #model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
        print(model.summary())
        self.model = model

    def fit(self, X_train, Y_train, epochs=500, batch_size=1):
        X_train = self.str_to_matrix(X_train)
        self.model.fit(X_train,Y_train, epochs=epochs, batch_size=batch_size)
  
    def predict(self, X_test):
        X_test = self.str_to_matrix(X_test)
        return self.model.predict(X_test)
    
    
    
#x_train = ['fdgdgdg', 'aeaehjfs', 'ijfkdfjkd+', 'fdgvMGKKG']
#y_train = [1,2,4,3]

# encode class values as integers
#encoder = LabelEncoder()
#encoder.fit(y_train)
#encoded_y_train = encoder.transform(y_train)
# convert integers to dummy variables (i.e. one hot encoded)
#dummy_y = to_categorical(encoded_y_train)
#y_train = dummy_y


#rnn = RNNClassifier(5, 4, {1:'A', 2:'B', 3:'C', 4:'D'}, 'lstm')
#rnn.built_dict_character(x_train)
#rnn.buil_model()
#rnn.fit(x_train, y_train)
#rnn.model.predict(rnn.str_to_matrix(x_test))
