package fs.common;
import java.util.regex.Pattern;

public class Configure {
	public static final Pattern rePunctuation = Pattern
			.compile("[^\\p{L}\\p{N}]"); // L:Letter, N:Number
	public static final Pattern reSpaceOrAnderscore = Pattern
			.compile("(_|\\s)+");
	public static int GramSize = 3;
	public static boolean RunOnCluster = true; // <<< muss unbedingt umgestellt werden für lokal, weil sonst das Einlesen des Distributed Cache Files nicht klappt
	public static boolean TokenAsWord = true; // unklar. Wenns auf False wäre würde der Index-Map nicht mehr funktionieren. Vermutlich unnötig.
	public static boolean PermitDuplicatesInSet = false;

	public static int partitionNumber = 11; // wird in IndexAndPartition gebraucht und beschreibt die Anzahl der Partitionen, die durch TF entstehen sollen. Laut Paper sollen das Anzahl Knoten - 1 viele sein.
	// add by rct at 20160314
	public static int parInitialNumber = 3; // wird in IndexAndPartition gebraucht. Ist aber überflüssig im Fall von Nutzung von TF
	public static boolean usingTF = true; // wenn das auf True steht werden die Pivots basierend auf der Token Frequency ausgewählt, sodass gleich große Partitionen entstehen
}
