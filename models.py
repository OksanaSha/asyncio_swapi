import asyncio

import sqlalchemy as sq
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import config


engine = create_async_engine(config.PG_DSN_ALC)
Base = declarative_base()


class CharacterModel(Base):
    __tablename__ = 'characters'
    id_in_swapi = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String)
    height = sq.Column(sq.String)
    mass = sq.Column(sq.String)
    hair_color = sq.Column(sq.String)
    skin_color = sq.Column(sq.String)
    eye_color = sq.Column(sq.String)
    birth_year = sq.Column(sq.String)
    gender = sq.Column(sq.String)
    homeworld = sq.Column(sq.String)
    films = sq.Column(sq.String)
    species = sq.Column(sq.String)
    vehicles = sq.Column(sq.String)
    starships = sq.Column(sq.String)
    # created
    # edited
    # url


    @classmethod
    async def create(cls, character_data, db_session):
        async with db_session() as session:
            async with session.begin():
                new_character = CharacterModel(**character_data)
                session.add(new_character)
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                new_character_data = {
                    'id': new_character.id_in_swapi,
                    'name': new_character.name
                }
                print(f'{new_character_data} successfully created')




async def get_async_session(drop: bool = False, create: bool = False):
    async with engine.begin() as conn:
        if drop:
            print('Dropping tables...')
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            print('Creating tables...')
            await conn.run_sync(Base.metadata.create_all)
    async_session_maker = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    return async_session_maker


async def main():
    db_session = await get_async_session(True, True)
    character = {'name': 'Anakin Skywalker', 'height': '188', 'mass': '84', 'hair_color': 'blond', 'skin_color': 'fair', 'eye_color': 'blue', 'birth_year': '41.9BBY', 'gender': 'male', 'homeworld': 'Tatooine', 'films': 'The Phantom Menace,Attack of the Clones,Revenge of the Sith', 'species': '', 'vehicles': 'Zephyr-G swoop bike,XJ-6 airspeeder', 'starships': 'Naboo fighter,Trade Federation cruiser,Jedi Interceptor', 'id_in_swapi': 11}
    await CharacterModel.create(character,db_session)


if __name__ == '__main__':
    asyncio.run(main())