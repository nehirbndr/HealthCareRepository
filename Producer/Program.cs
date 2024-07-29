using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

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

            try
            {
                // AdminClient ve Producer nesneleri oluşturulur.
                using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build())
                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    while (true)
                    {
                        try
                        {
                            List<string> topics = new List<string>();
                            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                            
                            // Mevcut topicler listelenir.
                            foreach (var topic in metadata.Topics)
                            {
                                if (!topic.Error.IsError)
                                {
                                    topics.Add(topic.Topic);
                                }
                            }

                            Console.WriteLine("Topicler:");
                            for (int i = 0; i < topics.Count; i++)
                            {
                                Console.WriteLine($"{i + 1}. {topics[i]}");
                            }

                            Console.Write("Bir topic seçin (yeni topic oluşturmak için 'yeni' yazın): ");
                            var input = Console.ReadLine();

                            string topicName;
                            if (input.ToLower() == "yeni")
                            {
                                Console.Write("Yeni topic adını girin: ");
                                topicName = Console.ReadLine();
                            }
                            else
                            {
                                // Mevcut topic seçilir.
                                int topicIndex = int.Parse(input) - 1;
                                topicName = topics[topicIndex];
                            }

                            Console.Write("Mesajı girin (çıkmak için 'çıkış' yazın): ");
                            var message = Console.ReadLine();

                            //Kullanıcının 'çıkış' yazması durumunda program sonlandırılır.
                            if (message == "çıkış")
                            {
                                break;
                            }

                            //Mesaj ilgili topice asenkron olarak gönderilir ve sonuç beklenir.
                            var result = producer.ProduceAsync(topicName, new Message<Null, string> { Value = message }).GetAwaiter().GetResult();
                            
                            //Mesajın teslim edildiği bilgisi ve konumu konsola yazdırılır.
                            Console.WriteLine($"'{result.Value}' mesajı '{result.TopicPartitionOffset}' konumuna teslim edildi.");
                        }
                        catch (ProduceException<Null, string> ex)
                        {
                            Console.WriteLine($"Mesaj gönderim hatası: {ex.Error.Reason}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Bir hata oluştu: {ex.Message}");
                        }
                    }
                }
            }
            catch (KafkaException ex)
            {
                Console.WriteLine($"Kafka ile ilgili bir hata oluştu: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Bir hata oluştu: {ex.Message}");
            }
        }
    }
}
