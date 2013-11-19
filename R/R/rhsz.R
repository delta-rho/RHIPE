#' Serialize and Unserialize
#' See Details.
#' 
#' The function \code{rhsz} serializes an object using RHIPE's binary
#' serialization. This will return the raw bytes corresponding the serialized
#' object. If the object cannot be serialized, it will be converted to NULL and
#' serialized. \code{rhuz} takes the bytes and un-serializes, throwing an error
#' if it cannot. These two functions are also available at the R console. RHIPE
#' uses the internals of these functions in \code{rhcollect} and \code{rhread}.
#' The maximum size of the serialized object that can be read is 256MB. Larger
#' objects will be written successfully, but when read RHIPE will throw an
#' error. These functions are useful to get an approximate idea of how large an
#' object will be.
#' 
#' The R serialization is verbose. Serialized objects have 22 bytes of header
#' and booleans are serialized to integers.  Best performance is achieved in
#' Hadoop when the size of the data exchanged is as small as possible. RHIPE
#' implements its own serialization using Google's
#' \href{http://code.google.com/p/protobuf/}{Protocol Buffers} . A benefit
#' of using this is that the data produced by RHIPE can be read in languages
#' such as Python, C and Java using the wrappers provided on the Google
#' website.
#' 
#' However, a drawback of RHIPE's serialization is that not all R objects can
#' be seamlessly serialized. RHIPE can serialize the following 
#' \itemize{ 
#' \item{
#' Scalar vectors: integers, characters (including UTF8 strings), numerics,
#' logicals, complex and raw. \code{NA} values are accepted.}
#' \item {Lists of the above.}
#' {\item Attributes of objects. RHIPE can serialize data frames, factors,
#' matrices (including others like time series objects) since these are the
#' above data structure with attributes.}
#' }
#' 
#' Closures, environments and promises cannot be serialized.  You may often get
#' around this limitation by using the built in serialization present in R
#' first, and then serializing that string via \code{rhsz} (or indirectly via
#' \code{rhcollect}. For example, to serialize the output of
#' \code{xyplot}, wrap it in a call to \code{serialize}.
#' 
#' @author Saptarshi Guha
#' @param r an object to serialize or unserialize
#' @return a serialized or unserialized object
#' @export
rhsz <- function(r) .Call("serializeUsingPB",r,PACKAGE="Rhipe")

#' Serialize and Unserialize
#' See Details.
#' 
#' The function \code{rhsz} serializes an object using RHIPE's binary
#' serialization. This will return the raw bytes corresponding the serialized
#' object. If the object cannot be serialized, it will be converted to NULL and
#' serialized. \code{rhuz} takes the bytes and un-serializes, throwing an error
#' if it cannot. These two functions are also available at the R console. RHIPE
#' uses the internals of these functions in \code{rhcollect} and \code{rhread}.
#' The maximum size of the serialized object that can be read is 256MB. Larger
#' objects will be written successfully, but when read RHIPE will throw an
#' error. These functions are useful to get an approximate idea of how large an
#' object will be.
#' 
#' The R serialization is verbose. Serialized objects have 22 bytes of header
#' and booleans are serialized to integers.  Best performance is achieved in
#' Hadoop when the size of the data exchanged is as small as possible. RHIPE
#' implements its own serialization using Google's
#' \href{http://code.google.com/p/protobuf/}{Protocol Buffers} . A benefit
#' of using this is that the data produced by RHIPE can be read in languages
#' such as Python, C and Java using the wrappers provided on the Google
#' website.
#' 
#' However, a drawback of RHIPE's serialization is that not all R objects can
#' be seamlessly serialized. RHIPE can serialize the following 
#' \itemize{ 
#' \item{
#' Scalar vectors: integers, characters (including UTF8 strings), numerics,
#' logicals, complex and raw. \code{NA} values are accepted.}
#' \item {Lists of the above.}
#' {\item Attributes of objects. RHIPE can serialize data frames, factors,
#' matrices (including others like time series objects) since these are the
#' above data structure with attributes.}
#' }
#' 
#' Closures, environments and promises cannot be serialized.  You may often get
#' around this limitation by using the built in serialization present in R
#' first, and then serializing that string via \code{rhsz} (or indirectly via
#' \code{rhcollect}. For example, to serialize the output of
#' \code{xyplot}, wrap it in a call to \code{serialize}.
#' 
#' @author Saptarshi Guha
#' @param r an object to serialize or unserialize
#' @return a serialized or unserialized object
#' @export
rhuz <- function(r) .Call("unserializeUsingPB",r,PACKAGE="Rhipe")

