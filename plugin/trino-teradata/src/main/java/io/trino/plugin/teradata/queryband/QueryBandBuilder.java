package io.trino.plugin.teradata.queryband;

import io.trino.plugin.teradata.util.TeradataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * QueryBandBuilder is responsible for constructing and validating query band strings
 * used in Teradata queries.
 * <p>
 * This utility class ensures that query band strings contain all required fields
 * and are formatted correctly according to Teradata specifications. It handles
 * user-provided query bands by validating and enhancing them with default values
 * when necessary.
 * </p>
 * <p>
 * Query bands are key-value pairs separated by semicolons that provide metadata
 * about queries executed in Teradata. This class ensures that essential fields
 * like 'org' and 'appname' are present and properly formatted.
 * </p>
 *
 * @author Teradata Plugin Team
 * @version 1.0
 * @since 1.0
 */
public class QueryBandBuilder {
//    private static final Logger LOGGER = LoggerFactory.getLogger(QueryBandBuilder.class);
    /** The current query band string, initialized with default values */
    private static String queryBand = TeradataConstants.DEFAULT_QUERY_BAND;

    /**
     * Handles user-provided query band text by ensuring required fields are present.
     * <p>
     * This method processes user input to ensure compliance with Teradata query band
     * requirements. It performs the following validations and enhancements:
     * </p>
     * <ul>
     *   <li>Adds default 'org' field if not present</li>
     *   <li>Ensures 'appname' contains 'trino' identifier</li>
     *   <li>Maintains proper semicolon formatting</li>
     * </ul>
     *
     * @param queryBandText user-provided query band string, may be null or empty
     * @return processed query band string with all required fields present,
     *         returns default query band if input is null or empty
     * @throws PatternSyntaxException if regex patterns fail (should not occur with current implementation)
     *
     * @example
     * <pre>
     * // Input: "appname=myapp;priority=high"
     * // Output: "appname=myapp_trino;priority=high;org=default_org"
     *
     * // Input: null or ""
     * // Output: default query band from TeradataConstants
     * </pre>
     */
    public static String handleUserQueryBandText(String queryBandText)
    {
        if (queryBandText == null || queryBandText.trim().isEmpty()) {
            return queryBand;
        }

        StringBuilder updatedQueryBand = new StringBuilder(queryBandText);

        // Check if 'org' doesn't exist in query_band, append default org
        Pattern orgPattern = Pattern.compile("org\\s*=");
        Matcher orgMatcher = orgPattern.matcher(queryBandText);
        if (!orgMatcher.find()) {
            if (!queryBandText.endsWith(";")) {
                updatedQueryBand.append(";");
            }
            updatedQueryBand.append(TeradataConstants.DEFAULT_QUERY_BAND_ORG);
        }

        // Ensure appname contains 'trino' or append it
        Pattern appNamePattern = Pattern.compile("appname\\s*=\\s*([^;]*)");
        Matcher appNameMatcher = appNamePattern.matcher(updatedQueryBand);
        if (appNameMatcher.find()) {
            String appNameValue = appNameMatcher.group(1).trim();
            if (!appNameValue.toLowerCase().contains("trino")) {
                String replacement = "appname=" + appNameValue + "_trino";
                updatedQueryBand = new StringBuilder(
                        updatedQueryBand.toString().replaceFirst("appname\\s*=\\s*([^;]*)", replacement)
                );
            }
        } else {
            if (!updatedQueryBand.isEmpty() && !updatedQueryBand.toString().endsWith(";")) {
                updatedQueryBand.append(";");
            }
            updatedQueryBand.append(TeradataConstants.DEFAULT_QUERY_BAND_APPNAME);
        }

        return updatedQueryBand.toString();
    }

    /**
     * Gets the current query band string.
     * <p>
     * Returns the currently configured query band, which may be the default
     * query band or a user-provided query band that has been processed through
     * {@link #setUserQueryBand(String)}.
     * </p>
     *
     * @return current query band string, never null
     */
    public static String getQueryBand()
    {
        return queryBand;
    }

    /**
     * Sets the user-provided query band after processing it for compliance.
     * <p>
     * This method processes the input through {@link #handleUserQueryBandText(String)}
     * to ensure all required fields are present before setting it as the current
     * query band.
     * </p>
     *
     * @param userQueryBand user-provided query band string, may be null or empty
     * @see #handleUserQueryBandText(String) for processing details
     *
     * @example
     * <pre>
     * QueryBandBuilder.setUserQueryBand("appname=custom;priority=high");
     * String current = QueryBandBuilder.getQueryBand();
     * // current will contain processed version with required fields
     * </pre>
     */
    public static void setUserQueryBand(String userQueryBand)
    {
        queryBand = handleUserQueryBandText(userQueryBand);
    }

}
