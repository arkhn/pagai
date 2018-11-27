
# Example of LSTM to learn a sequence
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM

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
            X_matrix[i, : sequence_i.shape[0], :] = sequence_i
        return X_matrix

    def buil_model(use_dropout=True, hidden_size=10, dropout=0.5):
        model = Sequential()
        # batch size, number of time steps, hidden size)
        model.add(Embedding(self.vocab_size, hidden_size=hidden_size, input_length=max_len_sequence))
        model.add(LSTM(hidden_size, return_sequences=False))
        #model.add(LSTM(hidden_size, return_sequences=True))
        if use_dropout:
            model.add(Dropout(dropout))
        model.add(TimeDistributed(Dense(self.vocab_size)))
        model.add(Activation('softmax'))
        model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['categorical_accuracy'])
        #model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
        print(model.summary())
        self.model = model

    def fit(self, X_train, Y_train):
        checkpointer = ModelCheckpoint(filepath=data_path + '/model-{epoch:02d}.hdf5', verbose=1)
        model.fit_generator(train_data_generator.generate(), len(train_data)//(batch_size*num_steps), num_epochs,
                        validation_data=valid_data_generator.generate(),
                        validation_steps=len(valid_data)//(batch_size*num_steps), callbacks=[checkpointer])
        #model.fit(X_train, y_train, validation_data=(X_test, y_test), epochs=3, batch_size=64)
    def predict(self, X_test):
        return self.model.predict(X_test)
