import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
async def run():
    client = AsyncIOMotorClient('mongodb://localhost:27017/')
    db = client['data_hygiene']
    
    pipeline = [
        {'$group': {
            '_id': '$benchmarkExecutionID',
            'invalidPayload': {'$first': '$invalidPayload'}
        }},
        {'$addFields': {
            'all_invalid_fields': {
                '$reduce': {
                    'input': {
                        '$map': {
                            'input': {'$ifNull': ['$invalidPayload', []]},
                            'as': 'p',
                            'in': {
                                '$concatArrays': [
                                    {'$cond': [{'$eq': ['$$p.validation_status', 'invalid']}, ['$$p.field'], []]},
                                    {
                                        '$map': {
                                            'input': {
                                                '$filter': {
                                                    'input': {'$ifNull': ['$$p.metadata', []]},
                                                    'as': 'm',
                                                    'cond': {'$eq': ['$$m.validation_status', 'invalid']}
                                                }
                                            },
                                            'as': 'm_valid',
                                            'in': '$$m_valid.name'
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    'initialValue': [],
                    'in': {'$concatArrays': ['$$value', '$$this']}
                }
            }
        }},
        {'$limit': 2}
    ]
    try:
        async for doc in db['Executioninfo'].aggregate(pipeline):
            print(doc.get('all_invalid_fields'))
    except Exception as e:
        import traceback
        traceback.print_exc()
        print('MongoError', e)
asyncio.run(run())
