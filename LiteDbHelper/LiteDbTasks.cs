using System;
using System.IO;
using LiteDB;

namespace LiteDbHelper
{
    public class LiteDbTasks
    {
        private readonly string ConnectionString;
        private LiteDatabase? liteDb;
        private static bool IsInstanceCreated = false;

        private LiteDbTasks(string ConnectionString)
        {
            this.ConnectionString = ConnectionString;
        }

        public static LiteDbTasks GetInstance(string ConnectionString)
        {
            if (!IsInstanceCreated) return new LiteDbTasks(ConnectionString);
            else throw new Exception("Instance Created Once.");
        }

        private void OpenDatabaseConnection()
        {
            liteDb = new LiteDatabase(ConnectionString);
        }

        private void CloseDataBaseConnection()
        {
            liteDb = null;
        }

        public LiteFileInfo<ObjectId> UploadFile(string FileAddress)
        {
            OpenDatabaseConnection();
            ILiteStorage<ObjectId> storage = liteDb.GetStorage<ObjectId>();
            LiteFileInfo<ObjectId> FileInfo = storage.Upload(ObjectId.NewObjectId(), FileAddress);
            CloseDataBaseConnection();
            return FileInfo;
        }

        public LiteFileInfo<ObjectId> UploadFile(Stream FileStream, string FileName)
        {
            OpenDatabaseConnection();
            ILiteStorage<ObjectId> storage = liteDb.GetStorage<ObjectId>();
            LiteFileInfo<ObjectId> FileInfo = storage.Upload(ObjectId.NewObjectId(), FileName, FileStream);
            CloseDataBaseConnection();
            return FileInfo;
        }

        public LiteFileInfo<ObjectId> UploadFile(byte[] FileByte, string FileName)
        {
            OpenDatabaseConnection();
            ILiteStorage<ObjectId> storage = liteDb.GetStorage<ObjectId>();
            LiteFileInfo<ObjectId> FileInfo = storage.Upload(ObjectId.NewObjectId(), FileName, new MemoryStream(FileByte));
            CloseDataBaseConnection();
            return FileInfo;
        }

        public byte[] DownloadFile(ObjectId objectId)
        {
            OpenDatabaseConnection();
            ILiteStorage<ObjectId> storage = liteDb.GetStorage<ObjectId>();
            Stream stream = Stream.Null;
            storage.Download(objectId, stream);
            return StreamToByteArray(stream);
        }
        
        public Stream DownloadFileStream(ObjectId objectId)
        {
            OpenDatabaseConnection();
            ILiteStorage<ObjectId> storage = liteDb.GetStorage<ObjectId>();
            Stream stream = Stream.Null;
            storage.Download(objectId, stream);
            return stream;
        }

        private byte[] StreamToByteArray(Stream input)
        {
            byte[] buffer = new byte[16 * 1024];
            using (MemoryStream ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }
    }
}