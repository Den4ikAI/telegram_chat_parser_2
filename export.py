from datasets import load_dataset
import orjson

def export_data(path):    
    
    data = load_dataset('csv', 
        data_files={
            'train': path + "/raw.csv"
        })
    # train_test_split
    data = data['train'].train_test_split(test_size=0.2)
    data = data.filter(lambda sample: sample['context_1'] is not None and sample['response'] is not None)
    # save_data
    with open(path + '/train.jsonl', 'wb') as dataset:
            train = data['train']
            for chunk in train:
                dataset.write(orjson.dumps(chunk, option=orjson.OPT_APPEND_NEWLINE))

    with open(path + '/test.jsonl', 'wb') as dataset:
            test  = data['test']
            for chunk in test:
                dataset.write(orjson.dumps(chunk, option=orjson.OPT_APPEND_NEWLINE))
