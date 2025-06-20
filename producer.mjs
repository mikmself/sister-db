import { Kafka } from "kafkajs";
import readline from "readline";

const kafka = new Kafka({
  clientId: "my-producer",
  brokers: ["192.168.0.105:9091", "192.168.0.106:9092", "192.168.0.103:9093"],
});

const producer = kafka.producer();

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const promptInput = () => {
  rl.question(
    'Pilih action (create/update/delete) atau "quit": ',
    async (action) => {
      if (action.toLowerCase() === "quit") {
        console.log("Producer dihentikan.");
        await producer.disconnect();
        rl.close();
        return;
      }

      if (!["create", "update", "delete"].includes(action.toLowerCase())) {
        console.log("Action tidak valid. Gunakan: create, update, atau delete");
        promptInput();
        return;
      }

      rl.question(
        'Masukkan data mahasiswa (format: nim, nama, email, prodi): ',
        async (input) => {
          const [nim, nama, email, prodi] = input.split(",");

          const mahasiswa = {
            action: action.toLowerCase(),
            nim: nim.trim(),
            nama: nama.trim(),
            email: email.trim(),
            prodi: prodi.trim(),
          };

          try {
            await producer.send({
              topic: "mhs",
              messages: [{ value: JSON.stringify(mahasiswa) }],
            });
            console.log("Data mahasiswa dikirim:", mahasiswa);
          } catch (error) {
            console.error("Gagal mengirim data:", error);
          }

          promptInput();
        }
      );
    }
  );
};

const run = async () => {
  await producer.connect();
  console.log("Producer terhubung ke Kafka");
  promptInput();
};

run().catch(console.error);
