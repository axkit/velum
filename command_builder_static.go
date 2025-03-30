package velum

func buildExistByPK(t, pk string, param string) string {
	return "SELECT EXISTS(SELECT 1 FROM " + t + " WHERE " + pk + "=" + param + ")"
}

func buildExist(t string, clauses string) string {
	return "SELECT EXISTS(SELECT 1 FROM " + t + " t " + clauses + ")"
}

func buildRowsCount(t string, clauses string) string {
	return "SELECT COUNT(*) FROM " + t + " t " + clauses
}

func buildTruncate(t string) string {
	return "TRUNCATE TABLE " + t
}

func buildHardDeleteByPK(t, pk string, param string) string {
	return "DELETE FROM " + t + " WHERE " + pk + "=" + param
}
