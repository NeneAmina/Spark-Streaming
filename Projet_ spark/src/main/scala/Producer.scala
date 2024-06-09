import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Producer {

  def main(args: Array[String]): Unit = {
    // Chemin absolu vers le fichier CSV
    val inputFile = "C:\\Users\\diall\\IdeaProjects\\Projet_ spark\\source_data\\prix-carburants-quotidien.csv" // Remplacez par le chemin absolu vers votre fichier CSV
    val linesPerSegment = 2000 // Nombre de lignes par segment
    val outputPath = "C:\\Users\\diall\\IdeaProjects\\Projet_ spark\\produced_data" // Remplacez par votre chemin absolu vers le répertoire de sortie

    // Configurer le répertoire Hadoop pour Windows avec un chemin absolu
    val hadoopHomeDir = "C:\\Users\\diall\\IdeaProjects\\Projet_ spark\\resources\\resources\\hadoop"
    System.setProperty("hadoop.home.dir", hadoopHomeDir)

    println(s"Hadoop home directory: $hadoopHomeDir")

    val spark = SparkSession.builder()
      .appName("Spark File Splitter")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val inputDF = spark.read.option("header", true).option("delimiter", ";").option("quote", "\"").csv(inputFile)
      println(s"Input file path: $inputFile")
      println(s"Number of lines in input DataFrame: ${inputDF.count()}")

      val segmentedDFs = splitFile(inputDF, linesPerSegment, outputPath)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  def splitFile(inputDF: DataFrame, linesPerSegment: Int, outputPath: String): Array[DataFrame] = {

    // Compter le nombre total de lignes dans le DataFrame
    val totalRows = inputDF.count()
    println(s"Total number of rows: $totalRows")

    // Calculer le nombre total de segments
    val totalSegments = Math.ceil(totalRows.toDouble / linesPerSegment).toInt
    println(s"Total number of segments: $totalSegments")

    // Créer un DataFrame pour chaque segment
    val segmentedDFs = (0 until totalSegments).map { segmentIndex =>
      // Calculer les numéros de ligne de début et de fin pour le segment actuel
      val startRow = segmentIndex * linesPerSegment
      val endRow = Math.min((segmentIndex + 1) * linesPerSegment - 1, totalRows - 1).toInt

      println(s"Processing segment $segmentIndex: rows $startRow to $endRow")

      // Ajouter une colonne d'index au DataFrame
      val indexedDF = inputDF.withColumn("index", monotonically_increasing_id())

      // Filtrer les données pour le segment actuel
      val segmentDF = indexedDF.filter(col("index").between(startRow, endRow))

      // Supprimer la colonne d'index
      val cleanedSegmentDF = segmentDF.drop("index")

      // Écrire le segment dans un fichier CSV//val segmentPath = s"$outputPath\\segment$segmentIndex"
      val segmentPath = s"$outputPath\\segment$segmentIndex"
      println(s"Writing segment to: $segmentPath")
      cleanedSegmentDF.write.mode("overwrite").option("header", true).csv(segmentPath)

      // Retourner le DataFrame du segment
      cleanedSegmentDF
    }
    segmentedDFs.toArray
  }
}
