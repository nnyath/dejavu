from __future__ import absolute_import

import logging
from itertools import izip_longest

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref, synonym
from sqlalchemy import create_engine, Column, LargeBinary, Integer, Text, Boolean, ForeignKey
from sqlalchemy.schema import UniqueConstraint, PrimaryKeyConstraint

from dejavu.database import Database

Base = declarative_base()

class Song(Base):
    __tablename__ = "songs"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)  # FIXME: should be a mediumint
    name = Column(Text, name=Database.FIELD_SONGNAME, nullable=False)
    fingerprinted = Column(Boolean, default=False)
    _file_sha1 = Column(LargeBinary(20), name=Database.FIELD_FILE_SHA1, nullable=False)

    UniqueConstraint(name, _file_sha1, name="unique_constraint")

    fingerprints = relationship("Fingerprint", backref="song", cascade="all,delete-orphan", passive_deletes=True)

    @property
    def file_sha1(self):
        return self._file_sha1.encode("hex")

    @file_sha1.setter
    def file_sha1(self, file_sha1):
        self._file_sha1 = file_sha1.decode("hex")

class Fingerprint(Base):
    __tablename__ = "fingerprints"

    _hash = Column(LargeBinary(10), name=Database.FIELD_HASH, index=True, nullable=False)
    song_id = Column(Integer, ForeignKey(Song.id,ondelete="CASCADE"), name=Database.FIELD_SONG_ID)
    song_offset = Column(Integer, name=Database.FIELD_OFFSET)

    PrimaryKeyConstraint(_hash, song_id, song_offset, name="pk_constraint")
    UniqueConstraint(_hash, song_id, song_offset, name="unique_constraint")

    @property
    def hash(self):
        return self._hash.encode("hex")

    @hash.setter
    def hash(self, hash):
        self._hash = hash.decode("hex")

    hash = synonym('_hash', descriptor=hash)

class SQLADatabase(Database):

    # Substantially similar to the mysql driver except made to use SQL Alchemy.

    # Name of your Database subclass, this is used in configuration
    # to refer to your class
    type = "sqlalchemy"

    # fields
    FIELD_FINGERPRINTED = "fingerprinted"

    Engine = None
    Session = None

    def __init__(self, **options):
        super(SQLADatabase, self).__init__()
        connection_string = ""
        if options.has_key('connection_string') and options['connection_string'] != "":
            connection_string = options['connection_string']
        else:
            connection_string = "{}://".format(options['driver'])
            if options.has_key('user') and options['user'] != "":
                connection_string += "{}:{}".format(options['user'],options['passwd'])
            if options.has_key('host') and options['host'] != "":
                connection_string += "@{}".format(options['host'])
            if options.has_key('db') and options['db'] != "":
                connection_string += "/{}".format(options['db'])
        self.Engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.Engine)

    @staticmethod
    def _song_to_dict(song):
        """
        Returns a song dict for dejavu to consume.

        If the song was not found, this method returns None.
        """
        return {
            SQLADatabase.FIELD_SONG_ID: song.id,
            SQLADatabase.FIELD_SONGNAME: song.name,
            SQLADatabase.FIELD_FINGERPRINTED: 1 if song.fingerprinted is True else 0,
            SQLADatabase.FIELD_FILE_SHA1: song.file_sha1.upper()
        } if song is not None else None

    @staticmethod
    def _grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return (filter(None, values) for values
                in izip_longest(fillvalue=fillvalue, *args))

    def setup(self):
        """
        Creates any non-existing tables required for dejavu to function.
        """
        Base.metadata.create_all(self.Engine)
        self.delete_unfingerprinted_songs()

    def empty(self):
        """
        Called when the database should be cleared of all data.
        """
        Fingerprint.__table__.drop()
        Song.__table__.drop()
        self.setup()

    def delete_unfingerprinted_songs(self):
        """
        Called to remove any song entries that do not have any fingerprints
        associated with them.
        """
        session = self.Session()
        session.query(Song).filter_by(fingerprinted=False).delete(synchronize_session=False)
        session.expire_all()
        session.close()

    def get_num_songs(self):
        """
        Returns the amount of songs in the database.
        """
        session = self.Session()
        count = session.query(Song).filter_by(fingerprinted=True).count()
        session.close()
        return count

    def get_num_fingerprints(self):
        """
        Returns the number of fingerprints in the database.
        """
        session = self.Session()
        count = session.query(Fingerprint).count()
        session.close()
        return count

    def set_song_fingerprinted(self, sid):
        """
        Sets a specific song as having all fingerprints in the database.

        sid: Song identifier
        """
        if sid is None:
            logging.warning("set_song_fingerprinted(): sid is None")
        else:
            session = self.Session()
            song = session.query(Song).get(sid)
            song.fingerprinted = True
            session.commit()

    def get_songs(self):
        """
        Returns all fully fingerprinted songs in the database.
        """
        session = self.Session()
        songs = session.query(Song).filter_by(fingerprinted=True).all()
        session.close()
        for s in songs:
            yield self._song_to_dict(s)

    def get_song_by_id(self, sid):
        """
        Return a song by its identifier

        sid: Song identifier
        """
        session = self.Session()
        song = session.query(Song).get(sid)
        session.close()
        return self._song_to_dict(song)

    def insert(self, hash, sid, offset):
        """
        Inserts a single fingerprint into the database.

          hash: Part of a sha1 hash, in hexadecimal format
           sid: Song identifier this fingerprint is off
        offset: The offset this hash is from
        """
        session = self.Session()
        fingerprint = Fingerprint(hash=hash, song_id=sid, song_offset=offset)
        session.add(fingerprint)
        session.commit()
        session.close()

    def insert_song(self, song_name, sha1_hash):
        """
        Inserts a song name into the database, returns the new
        identifier of the song.

        song_name: The name of the song.
        """
        session = self.Session()
        song = session.query(Song).filter_by(name=song_name).first()
        if song is not None:
            return song.id
        else:
            song = Song(name=song_name, file_sha1=sha1_hash)
            session.add(song)
            session.commit()
            return song.id

    def query(self, hash):
        """
        Returns all matching fingerprint entries associated with
        the given hash as parameter.

        hash: Part of a sha1 hash, in hexadecimal format
        """
        session = self.Session()
        fingerprints = [(f.sid, f.song_offset) for f in session.query(Fingerprint).filter_by(hash=hash).all()]
        session.close()
        return fingerprints

    def get_iterable_kv_pairs(self):
        """
        Returns all fingerprints in the database.
        """
        session = self.Session()
        fingerprints = [(f.sid, f.song_offset) for f in session.query(Fingerprint).all()]
        session.close()
        return fingerprints

    def insert_hashes(self, sid, hashes):
        """
        Insert a multitude of fingerprints.

           sid: Song identifier the fingerprints belong to
        hashes: A sequence of tuples in the format (hash, offset)
        -   hash: Part of a sha1 hash, in hexadecimal format
        - offset: Offset this hash was created from/at.
        """
        session = self.Session()
        for h in hashes:
            fingerprint = Fingerprint(hash=h[0], song_id=sid, song_offset=int(h[1]))
            session.add(fingerprint)
        session.commit()

    def return_matches(self, hashes):
        """
        Searches the database for pairs of (hash, offset) values.

        hashes: A sequence of tuples in the format (hash, offset)
        -   hash: Part of a sha1 hash, in hexadecimal format
        - offset: Offset this hash was created from/at.

        Returns a sequence of (sid, offset_difference) tuples.

                      sid: Song identifier
        offset_difference: (offset - database_offset)
        """
        session = self.Session()
        mapper = {}
        for hash, offset in hashes:
            mapper[hash.decode("hex")] = offset
        fingerprints = []
        for split in self._grouper(mapper.keys(), 999):
            fingerprints += session.query(Fingerprint).filter(Fingerprint._hash.in_(split)).all()
        session.close()
        for f in fingerprints:
            yield (f.song_id, f.song_offset-mapper[f._hash])

