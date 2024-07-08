using System;
using Confluent.Kafka;

namespace HealthCareManagement.Producer
{
    class Producer
    {
        static void Main(string[] args)
        {
            var config = new ProducerConfig
            {
                
                //Kafka broker adresi ve portu belirtilir.
                BootstrapServers = "localhost:9092"
            };

            //Üretici nesnesi oluşturulur ve yapılandırılır.
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                //Kullanıcıdan sürekli olarak mesaj alınır ve Kafka'ya gönderilir.
                while (true) 
                {
                    Console.Write("Mesajı girin (çıkmak için 'çıkış' yazın): ");
                    var message = Console.ReadLine();
                    
                    //Kullanıcının 'exit' yazması durumunda program sonlandırılır.
                    if (message == "çıkış")
                    {
                        break;
                    }
                    
                    //Mesaj ilgili topic'e asenkron olarak gönderilir ve sonuç beklenir.
                    var result = producer.ProduceAsync("healthcare-topic", new Message<Null, string> { Value = message }).GetAwaiter().GetResult();
                    
                    //Mesajın teslim edildiği bilgisi ve konumu konsola yazdırılır.
                    Console.WriteLine($"'{result.Value}' mesajı '{result.TopicPartitionOffset}' konumuna teslim edildi.");
                }
            }
        }
    }
}