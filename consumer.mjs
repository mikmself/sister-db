import { Kafka } from "kafkajs";
import mysql from "mysql2/promise";
const db = await mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "password",
  database: "kampus",
});

const kafka = new Kafka({
  clientId: "irga",
  brokers: ["192.168.0.105:9091", "192.168.0.106:9092", "192.168.0.103:9093"],
});

const consumer = kafka.consumer({ groupId: "irga" });
const run = async () => {
  await consumer.connect();
  console.log("Consumer connected");
  await consumer.subscribe({
    topic: "mhs",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log("Data diterima:", data);

      try {
        const { action, nim, nama, email, prodi } = data;

        switch (action) {
          case "create":
            const checkSql = "SELECT COUNT(*) as count FROM mahasiswa WHERE nim = ?";
            const [rows] = await db.execute(checkSql, [nim]);

            if (rows[0].count > 0) {
              console.log(`Data dengan NIM ${nim} sudah ada, skip insert.`);
              return;
            }
            const insertSql = "INSERT INTO mahasiswa (nim, nama, email, prodi) VALUES(?, ?, ?, ?)";
            await db.execute(insertSql, [nim, nama, email, prodi]);
            console.log("Data mahasiswa berhasil dibuat.");
            break;

          case "update":
            const checkUpdateSql = "SELECT COUNT(*) as count FROM mahasiswa WHERE nim = ?";
            const [updateRows] = await db.execute(checkUpdateSql, [nim]);

            if (updateRows[0].count === 0) {
              console.log(`Data dengan NIM ${nim} tidak ditemukan, skip update.`);
              return;
            }
            const updateSql = "UPDATE mahasiswa SET nama = ?, email = ?, prodi = ? WHERE nim = ?";
            await db.execute(updateSql, [nama, email, prodi, nim]);
            console.log("Data mahasiswa berhasil diupdate.");
            break;

          case "delete":
            const checkDeleteSql = "SELECT COUNT(*) as count FROM mahasiswa WHERE nim = ?";
            const [deleteRows] = await db.execute(checkDeleteSql, [nim]);

            if (deleteRows[0].count === 0) {
              console.log(`Data dengan NIM ${nim} tidak ditemukan, skip delete.`);
              return;
            }
            const deleteSql = "DELETE FROM mahasiswa WHERE nim = ?";
            await db.execute(deleteSql, [nim]);
            console.log("Data mahasiswa berhasil dihapus.");
            break;

          default:
            console.log("Action tidak valid:", action);
            break;
        }
      } catch (err) {
        console.error("Gagal memproses data:", err);
      }
    },
  });
};

run().catch(console.error);
